#include "tea/reader.h"

#include <arrow/type_fwd.h>
#include <iceberg/common/batch.h>
#include <iceberg/common/error.h>
#include <iceberg/common/fs/file_reader_provider.h>
#include <iceberg/common/logger.h>
#include <iceberg/common/selection_vector.h>
#include <iceberg/filter/representation/column_extractor.h>
#include <iceberg/filter/representation/visitor.h>
#include <iceberg/result.h>
#include <iceberg/streams/iceberg/data_entries_meta_stream.h>
#include <iceberg/streams/iceberg/parquet_stats_getter.h>
#include <iconv.h>
#include <parquet/arrow/reader.h>
#include <parquet/schema.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/strings/ascii.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/string.h"
#include "arrow/util/thread_pool.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/common/fs/filesystem_wrapper.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/serializer.h"
#include "iceberg/filter/row_filter/registry.h"
#include "iceberg/filter/row_filter/row_filter.h"
#include "iceberg/filter/stats_filter/stats_filter.h"
#include "iceberg/streams/filter/row_filter.h"
#include "iceberg/streams/iceberg/builder.h"
#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
#include "tea/common/file_reader_provider.h"
#include "tea/common/row_groups_utils.h"
#include "tea/debug/stats_state.grpc.pb.h"
#include "tea/debug/stats_state.pb.h"
#include "tea/debug/stats_to_proto.h"
#include "tea/metadata/metadata.h"
#include "tea/observability/reader_stats.h"
#include "tea/observability/return_fl.h"
#include "tea/observability/s3_stats.h"
#include "tea/observability/tea_log.h"
#include "tea/table/bridge.h"
#include "tea/table/filter_convert.h"
#include "tea/table/gp_funcs.h"
#include "tea/table/shared_state.h"
#include "tea/util/defer.h"
#include "tea/util/logger.h"
#include "tea/util/measure.h"
#include "tea/util/multishot_timer.h"
#include "tea/util/signal_blocker.h"

namespace tea {

namespace {

arrow::Status MatchIcebergSchema(const iceberg::Schema& schema, const std::vector<ReaderColumn>& columns) {
  std::unordered_map<std::string, const iceberg::types::NestedField*> fields;
  for (const auto& f : schema.Columns()) {
    fields.emplace(absl::AsciiStrToLower(f.name), &f);
  }
  for (const auto& c : columns) {
    auto it = fields.find(absl::AsciiStrToLower(c.gp_name));
    if (it == fields.end()) {
      return arrow::Status::ExecutionError("Greenplum column '", c.gp_name, "' not found in Iceberg schema");
    }
    auto maybe_field_id = MatchIcebergColumn(*it->second, c.gp_type);
    if (!maybe_field_id) {
      return arrow::Status::ExecutionError("Greenplum column '", c.gp_name, "' with type '",
                                           FormatTypeWithTypeMode(c.gp_type, c.gp_type_mode), "' (oid = ", c.gp_type,
                                           ") has incompatible Iceberg type ", it->second->type->ToString());
    }
  }
  return arrow::Status::OK();
}

// copypasted from metadata/access_iceberg.cpp with some changes
// TODO(gmusya): unify code
using FilesystemStats = Reader::FilesystemStats;

class LoggingInputFile : public iceberg::InputFileWrapper {
 public:
  LoggingInputFile(std::shared_ptr<arrow::io::RandomAccessFile> file, std::shared_ptr<FilesystemStats> metrics)
      : InputFileWrapper(file), metrics_(metrics) {}

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    metrics_->wait_read_stats->Resume();
    Defer defer([&]() { metrics_->wait_read_stats->Suspend(); });
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::ReadAt(position, nbytes, out);
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    metrics_->wait_read_stats->Resume();
    Defer defer([&]() { metrics_->wait_read_stats->Suspend(); });
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes, out);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    metrics_->wait_read_stats->Resume();
    Defer defer([&]() { metrics_->wait_read_stats->Suspend(); });
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes);
  }

 private:
  void TakeRequestIntoAccount(int64_t bytes) {
    std::lock_guard lock(metrics_->s3_read_stats_lock_);
    ++metrics_->s3_stats.requests;
    metrics_->s3_stats.bytes_read += bytes;
  }

  std::shared_ptr<FilesystemStats> metrics_;
};

class IcebergLoggingFileSystem : public iceberg::FileSystemWrapper {
 public:
  IcebergLoggingFileSystem(std::shared_ptr<arrow::fs::FileSystem> fs, std::shared_ptr<FilesystemStats> metrics)
      : FileSystemWrapper(fs), metrics_(metrics) {}

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(const std::string& path) override {
    ARROW_ASSIGN_OR_RAISE(auto file, FileSystemWrapper::OpenInputFile(path));
    return std::make_shared<LoggingInputFile>(file, metrics_);
  }

 private:
  std::shared_ptr<FilesystemStats> metrics_;
};

class LoggingFileSystemProvider : public iceberg::IFileSystemProvider {
 public:
  LoggingFileSystemProvider(std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
                            std::shared_ptr<FilesystemStats> metrics)
      : fs_provider_(fs_provider), metrics_(metrics) {}

  arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> GetFileSystem(const std::string& url) override {
    ARROW_ASSIGN_OR_RAISE(auto fs, fs_provider_->GetFileSystem(url));
    return std::make_shared<IcebergLoggingFileSystem>(fs, metrics_);
  }

 private:
  std::shared_ptr<iceberg::IFileSystemProvider> fs_provider_;
  std::shared_ptr<FilesystemStats> metrics_;
};

}  // namespace

Reader::Reader(const Config& config, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider) {
  if (!GetSharedState()) {
    throw arrow::Status::ExecutionError("Shared state is not initialized");
  }

  config_ = config;

  fs_stats_ = std::make_shared<FilesystemStats>();
  fs_stats_->wait_read_stats = std::make_shared<OneThreadMultishotTimer>();

  fs_provider_ = std::make_shared<LoggingFileSystemProvider>(fs_provider, fs_stats_);
}

Reader::~Reader() {}

arrow::Status Reader::Finalize() {
  auto thread_pool = static_cast<arrow::internal::ThreadPool*>(arrow::io::default_io_context().executor());
  bool wait_for_pending = false;
  (void)thread_pool->Shutdown(wait_for_pending);
  (void)arrow::internal::GetCpuThreadPool()->Shutdown(wait_for_pending);
  FinalizeSharedState();
  // TODO(hvintus): log the errors out
  return arrow::fs::EnsureS3Finalized();
}

static arrow::Status EnsureThreadsCreated(arrow::internal::ThreadPool* pool) {
  auto capacity = pool->GetCapacity();
  if (!(0 <= capacity && capacity <= 1000)) {
    return arrow::Status::ExecutionError("Unexpected thread pool capacity: ", capacity);
  }

  constexpr int kWaitState = 0;
  constexpr int kRunState = 1;

  std::atomic<int> wait_token{kWaitState};
  std::atomic<int> tasks_to_wait{0};

  auto busy_wait = [&]() {
    while (wait_token.load() != kRunState) {
    }
    tasks_to_wait.fetch_sub(1);
  };

  while (pool->GetNumTasks() < capacity) {
    tasks_to_wait.fetch_add(1);
    ARROW_RETURN_NOT_OK(pool->Submit(busy_wait));
  }

  wait_token.store(kRunState);
  while (tasks_to_wait.load() > 0) {
  }

  return arrow::Status::OK();
}

arrow::Status Reader::Initialize(int db_encoding) {
  SignalBlocker blocker;

  InitializeSharedState();

  Config config = ConfigSource::GetConfig();

  // Max threads for CPU-bound tasks.
  if (auto max_threads = config.limits.max_cpu_threads) {
    auto cpu_thread_pool = arrow::internal::GetCpuThreadPool();
    RETURN_FL_NOT_OK(cpu_thread_pool->SetCapacity(max_threads));
  }

  // Max threads for IO-bound tasks.
  if (auto max_threads = config.limits.max_io_threads) {
    auto thread_pool = static_cast<arrow::internal::ThreadPool*>(arrow::io::default_io_context().executor());
    RETURN_FL_NOT_OK(thread_pool->SetCapacity(max_threads));
  }

  RETURN_FL_NOT_OK(EnsureThreadsCreated(arrow::internal::GetCpuThreadPool()));
  RETURN_FL_NOT_OK(
      EnsureThreadsCreated(static_cast<arrow::internal::ThreadPool*>(arrow::io::default_io_context().executor())));

  if (auto status = GetSharedState()->InitializeConverter(db_encoding); !status.ok()) {
    return status;
  }

  return arrow::Status::OK();
}

using DataEntry = iceberg::ice_tea::DataEntry;

namespace {

class GandivaFilterApplier : public iceberg::ice_filter::IRowFilter {
 public:
  GandivaFilterApplier(std::shared_ptr<iceberg::filter::RowFilter> filter,
                       std::shared_ptr<const iceberg::filter::IGandivaFunctionRegistry> registry,
                       std::shared_ptr<iceberg::ILogger> logger, bool must_apply_filter,
                       std::vector<int32_t> involved_field_ids)
      : iceberg::ice_filter::IRowFilter(std::move(involved_field_ids)),
        filter_(filter),
        registry_(registry),
        logger_(logger),
        must_apply_filter_(must_apply_filter) {
    iceberg::Ensure(filter_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": filter is nullptr");
    iceberg::Ensure(registry_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": registry is nullptr");
  }

  iceberg::SelectionVector<int32_t> ApplyFilter(
      std::shared_ptr<iceberg::ArrowBatchWithRowPosition> batch) const override {
    auto schema = batch->GetRecordBatch()->schema();
    iceberg::Ensure(schema != nullptr, std::string(__PRETTY_FUNCTION__) + ": schema is nullptr");

    if (!old_schema_ || !old_schema_->Equals(schema)) {
      ScopedLogger scoped_log(logger_, "events:filter:start_build", "events:filter:end_build");

      auto field_resolver = std::make_shared<iceberg::filter::TrivialArrowFieldResolver>(schema);
      auto status = filter_->BuildFilter(field_resolver, registry_, schema);

      filter_is_built_ = status.ok();
      if (must_apply_filter_ && !filter_is_built_) {
        throw status;
      }

      old_schema_ = schema;
    }

    if (filter_is_built_) {
      ScopedLogger scoped_log(logger_, "events:filter:start_apply", "events:filter:end_apply");

      // current solution evaluates filter, creates selection vector and merges it with existing selection vector
      // TODO(gmusya): consider evaluating filter and merging with existing selection vector in one operation (compile
      // filter with checking selection vector bit)
      auto gandiva_selection_vector = iceberg::ValueSafe(filter_->ApplyFilter(batch->GetRecordBatch()));
      auto filtered_indices = gandiva_selection_vector->ToArray();
      iceberg::Ensure(filtered_indices != nullptr, std::string(__PRETTY_FUNCTION__) + ": filtered_indices is nullptr");

      auto indices_array = std::dynamic_pointer_cast<const arrow::UInt32Array>(filtered_indices);
      iceberg::Ensure(indices_array != nullptr, std::string(__PRETTY_FUNCTION__) + ": indices_array is nullptr");

      std::vector<int32_t> indices;
      indices.reserve(indices_array->length());
      for (const auto& x : *indices_array) {
        indices.emplace_back(x.value());
      }
      if (logger_) {
        logger_->Log(std::to_string(batch->GetRecordBatch()->num_rows() - static_cast<int32_t>(indices.size())),
                     "metrics:row_filter:deleted_rows");
      }

      return iceberg::SelectionVector<int32_t>(std::move(indices));
    }

    return iceberg::SelectionVector<int32_t>(batch->GetRecordBatch()->num_rows());
  }

 private:
  mutable std::shared_ptr<arrow::Schema> old_schema_;

  mutable bool filter_is_built_ = false;
  std::shared_ptr<iceberg::filter::RowFilter> filter_;
  std::shared_ptr<const iceberg::filter::IGandivaFunctionRegistry> registry_;
  std::shared_ptr<iceberg::ILogger> logger_;

  const bool must_apply_filter_ = false;
};

class RowGroupFilter : public iceberg::IRowGroupFilter {
  class RowGroupFilterLogger : public iceberg::filter::StatsFilter::ILogger {
   public:
    void Log(const std::string& message) override {
      if (already_logged_messages_.contains(message)) {
        return;
      }
      if (already_logged_messages_.size() >= kMaxLoggedMessagesCount) {
        // too many messages
        return;
      }
      if (already_logged_messages_.size() + 1 == kMaxLoggedMessagesCount) {
        TEA_LOG("RowGroupFilterLogger contains too many messages");
      }
      TEA_LOG(message);
      already_logged_messages_.insert(message);
    }

   private:
    static constexpr size_t kMaxLoggedMessagesCount = 100;
    std::unordered_set<std::string> already_logged_messages_;
  };

 public:
  RowGroupFilter(iceberg::filter::NodePtr extracted_filter_representation,
                 absl::flat_hash_map<int32_t, std::string> field_id_to_name, int64_t timestamp_to_timestamptz_shift_us)
      : extracted_filter_representation_(extracted_filter_representation),
        field_id_to_name_(std::move(field_id_to_name)),
        timestamp_to_timestamptz_shift_us_(timestamp_to_timestamptz_shift_us),
        logger_(std::make_shared<RowGroupFilterLogger>()) {}

  bool CanSkipRowGroup(const parquet::RowGroupMetaData& meta) const override {
    iceberg::filter::StatsFilter filter(extracted_filter_representation_,
                                        iceberg::filter::StatsFilter::Settings{.timestamp_to_timestamptz_shift_us =
                                                                                   timestamp_to_timestamptz_shift_us_});
    filter.SetLogger(logger_);
    return filter.ApplyFilter(GetCurrentParquetStatsGetter(meta)) == iceberg::filter::MatchedRows::kNone;
  }

 private:
  iceberg::ParquetStatsGetter GetCurrentParquetStatsGetter(const parquet::RowGroupMetaData& meta) const {
    const parquet::SchemaDescriptor* schema = meta.schema();
    iceberg::Ensure(schema != nullptr, std::string(__PRETTY_FUNCTION__) + ": internal error. schema is nullptr");

    const parquet::schema::GroupNode* group_node = schema->group_node();
    iceberg::Ensure(group_node != nullptr,
                    std::string(__PRETTY_FUNCTION__) + ": internal error. group_node is nullptr");

    int field_count = group_node->field_count();

    std::unordered_map<std::string, int> gp_name_to_parquet_index;

    for (int i = 0; i < field_count; ++i) {
      parquet::schema::NodePtr field_descr = group_node->field(i);
      iceberg::Ensure(field_descr != nullptr,
                      std::string(__PRETTY_FUNCTION__) + ": internal error. field_descr is nullptr");

      int32_t field_id = field_descr->field_id();
      if (field_id == -1) {
        continue;
      }

      if (field_id_to_name_.contains(field_id)) {
        gp_name_to_parquet_index[field_id_to_name_.at(field_id)] = i;
      }
    }

    return iceberg::ParquetStatsGetter(meta, gp_name_to_parquet_index);
  }

  iceberg::filter::NodePtr extracted_filter_representation_;
  const absl::flat_hash_map<int32_t, std::string> field_id_to_name_;
  const int64_t timestamp_to_timestamptz_shift_us_;
  std::shared_ptr<RowGroupFilterLogger> logger_;
};

struct LoggingAnnotatedDataPathStream : public iceberg::IAnnotatedDataPathStream {
 public:
  using Callback = std::function<void(std::shared_ptr<iceberg::AnnotatedDataPath>)>;

  LoggingAnnotatedDataPathStream(std::shared_ptr<iceberg::IAnnotatedDataPathStream> stream, const Callback& callback)
      : stream_(stream), callback_(callback) {}

  std::shared_ptr<iceberg::AnnotatedDataPath> ReadNext() override {
    auto result = stream_->ReadNext();
    callback_(result);
    return result;
  }

 private:
  std::shared_ptr<iceberg::IAnnotatedDataPathStream> stream_;
  Callback callback_;
};

}  // namespace

std::shared_ptr<Logger> Reader::InitializeLogger() {
  std::shared_ptr<Logger> logger = std::make_shared<Logger>();

  logger->SetHandler("metrics:equality:deleted_rows", [&](const Logger::Message& message) {
    stats_.rows_skipped_equality_delete += std::stoll(message);
  });
  logger->SetHandler("metrics:equality:files_read",
                     [&](const Logger::Message& message) { equality_delete_stats_.files_read += std::stoll(message); });
  logger->SetHandler("metrics:equality:rows_read",
                     [&](const Logger::Message& message) { equality_delete_stats_.rows_read += std::stoll(message); });
  logger->SetHandler("metrics:equality:current_materialized_rows", [&](const Logger::Message& message) {
    equality_delete_stats_.max_rows_materialized =
        std::max(equality_delete_stats_.max_rows_materialized, static_cast<uint64_t>(std::stoull(message)));
  });
  logger->SetHandler("metrics:equality:current_mb_materialized", [&](const Logger::Message& message) {
    equality_delete_stats_.max_mb_size_materialized =
        std::max(equality_delete_stats_.max_mb_size_materialized, std::stod(message));
  });

  logger->SetHandler("metrics:positional:deleted_rows", [&](const Logger::Message& message) {
    stats_.rows_skipped_positional_delete += std::stoll(message);
  });
  logger->SetHandler("metrics:positional:rows_read", [&](const Logger::Message& message) {
    positional_delete_stats_.rows_read += std::stoll(message);
  });
  logger->SetHandler("metrics:positional:files_read", [&](const Logger::Message& message) {
    positional_delete_stats_.files_read += std::stoll(message);
  });
  logger->SetHandler("metrics:positional:rows_skipped", [&](const Logger::Message& message) {
    stats_.positional_delete_rows_ignored += std::stoll(message);
  });

  logger->SetHandler("data_stream:metrics:row_groups:read",
                     [&](const Logger::Message& message) { stats_.row_groups_read += std::stoll(message); });
  logger->SetHandler("metrics:row_groups:skipped",
                     [&](const Logger::Message& message) { stats_.row_groups_skipped_filter += std::stoll(message); });

  logger->SetHandler("metrics:row_filter:deleted_rows",
                     [&](const Logger::Message& message) { stats_.rows_skipped_filter += std::stoll(message); });

  logger->SetHandler("metrics:data:files_read",
                     [&](const Logger::Message& message) { stats_.data_files_read += std::stoll(message); });

  logger->SetHandler("metrics:data:columns_equality_delete", [&](const Logger::Message& message) {
    stats_.columns_equality_delete =
        std::max(stats_.columns_equality_delete, static_cast<int64_t>(std::stoll(message)));
  });
  logger->SetHandler("metrics:data:columns_only_equality_delete", [&](const Logger::Message& message) {
    stats_.columns_only_for_equality_delete =
        std::max(stats_.columns_only_for_equality_delete, static_cast<int64_t>(std::stoll(message)));
  });
  logger->SetHandler("data_stream:metrics:data:columns_read", [&, used = 0](const Logger::Message& message) mutable {
    if (!used) {
      used = 1;
      stats_.columns_read += std::stoll(message);
    }
  });
  logger->SetHandler("filter_stream:metrics:data:columns_read", [&, used = 0](const Logger::Message& message) mutable {
    if (!used) {
      used = 1;
      stats_.columns_read += std::stoll(message);
    }
  });
  logger->SetHandler("metrics:rows:filtered_out", [&](const Logger::Message& message) mutable {
    stats_.rows_skipped_prefilter += std::stoll(message);
  });

  logger->SetHandler("events:positional:start_batch",
                     [&](const Logger::Message& message) { positional_delete_timer_.Resume(); });
  logger->SetHandler("events:positional:end_batch",
                     [&](const Logger::Message& message) { positional_delete_timer_.Suspend(); });

  logger->SetHandler("events:equality:start_batch",
                     [&](const Logger::Message& message) { equality_delete_timer_.Resume(); });
  logger->SetHandler("events:equality:end_batch",
                     [&](const Logger::Message& message) { equality_delete_timer_.Suspend(); });

  logger->SetHandler("events:filter:start_build",
                     [&](const Logger::Message& message) { filter_build_timer_.Resume(); });
  logger->SetHandler("events:filter:end_build", [&](const Logger::Message& message) { filter_build_timer_.Suspend(); });

  logger->SetHandler("events:filter:start_apply",
                     [&](const Logger::Message& message) { filter_apply_timer_.Resume(); });
  logger->SetHandler("events:filter:end_apply", [&](const Logger::Message& message) { filter_apply_timer_.Suspend(); });

  logger->SetHandler("row_filter:condition", [&](const Logger::Message& message) { TEA_LOG("Condition: " + message); });

  return logger;
}

static iceberg::EqualityDeleteHandler::Config MakeIcebergEqualityDeleteConfig(const Config& config) {
  iceberg::EqualityDeleteHandler::Config result;
  result.equality_delete_max_mb_size = config.limits.equality_delete_max_mb_size;
  result.max_rows = config.limits.equality_delete_max_rows;
  result.use_specialized_deletes = config.features.use_specialized_deletes;
  result.throw_if_memory_limit_exceeded = config.features.throw_if_memory_limit_exceeded;

  return result;
}

class LowercaseRenamingStream : public iceberg::IcebergStream {
 public:
  explicit LowercaseRenamingStream(std::shared_ptr<iceberg::IcebergStream> input) : input_(input) {}

  std::shared_ptr<iceberg::IcebergBatch> ReadNext() override {
    auto batch = input_->ReadNext();
    if (!batch) {
      return nullptr;
    }

    std::shared_ptr<arrow::RecordBatch> arrow_batch = [&]() {
      auto names = batch->GetRecordBatch()->schema()->field_names();
      for (auto& elem : names) {
        elem = arrow::internal::AsciiToLower(elem);
      }
      auto new_schema = iceberg::ValueSafe(batch->GetRecordBatch()->schema()->WithNames(names));

      return iceberg::ValueSafe(batch->GetRecordBatch()->ReplaceSchema(new_schema));
    }();

    return std::make_shared<iceberg::IcebergBatch>(
        iceberg::BatchWithSelectionVector(arrow_batch, std::move(batch->GetSelectionVector())),
        batch->GetPartitionLayerFilePosition());
  }

 private:
  std::shared_ptr<iceberg::IcebergStream> input_;
};

arrow::Status Reader::Plan(meta::PlannedMeta meta, const Reader::SerializedFilter& filter, bool postfilter_on_gp) {
  schema_ = meta.GetDeletes().schema;
  iceberg::Ensure(schema_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": schema is nullptr");

  // empty table
  if (schema_->Columns().empty()) {
    return arrow::Status::OK();
  }
  RETURN_FL_NOT_OK(MatchIcebergSchema(*schema_, columns_));

  iceberg::filter::NodePtr extracted_filter_representation_ = iceberg::filter::StringToFilter(filter.extracted);

  // used only for samovar logging purposes. This class has method UpdateMetrics, which updates metrics in samovar case
  // TODO(gmusya): handle samovar metrics outside of reader.cpp
  entries_stream_ = meta.GetStream();

  iceberg::AnnotatedDataPathStreamPtr meta_stream = std::make_shared<LoggingAnnotatedDataPathStream>(
      entries_stream_, [&](std::shared_ptr<iceberg::AnnotatedDataPath> value) {
        if (value) {
          ++stats_.samovar_fetched_tasks_count;
        }
      });
  iceberg::PositionalDeletes positional_deletes;
  auto equality_deletes = std::make_shared<iceberg::EqualityDeletes>(iceberg::EqualityDeletes{});
  iceberg::EqualityDeleteHandler::Config equality_delete_config = MakeIcebergEqualityDeleteConfig(config_);

  for (size_t partition_id = 0; partition_id < meta.GetDeletes().partitions.size(); ++partition_id) {
    auto& partition = meta.GetDeletes().partitions.at(partition_id);
    for (size_t layer_id = 0; layer_id < partition.size(); ++layer_id) {
      auto& layer = partition[layer_id];
      if (!layer.positional_delete_entries_.empty()) {
        positional_deletes.delete_entries[partition_id][layer_id] = std::move(layer.positional_delete_entries_);
      }
      if (!layer.equality_delete_entries_.empty()) {
        equality_deletes->partlayer_to_deletes[partition_id][layer_id] = std::move(layer.equality_delete_entries_);
      }
    }
  }

  std::vector<int> field_ids_to_retrieve;
  absl::flat_hash_map<int32_t, std::string> id_to_name;
  for (auto& col : columns_) {
    auto field_id = schema_->FindColumnIgnoreCase(col.gp_name);
    if (field_id == std::nullopt) {
      return arrow::Status::ExecutionError("Column with name '", col.gp_name, "' not found");
    }
    id_to_name[field_id.value()] = col.gp_name;

    field_ids_to_retrieve.emplace_back(field_id.value());
  }

  std::shared_ptr<const iceberg::IRowGroupFilter> rg_filter;
  if (extracted_filter_representation_) {
    rg_filter =
        std::make_shared<RowGroupFilter>(extracted_filter_representation_, id_to_name, TimestampToTimestamptzShiftUs());
  }

  std::shared_ptr<Logger> logger_ = InitializeLogger();

  {
    stats_.columns_for_greenplum = 0;
    for (const auto& column : columns_) {
      if (!column.remote_only) {
        ++stats_.columns_for_greenplum;
      }
    }
  }

  std::shared_ptr<iceberg::ice_filter::IRowFilter> ice_filter;
  if (!filter.row.empty()) {
    auto row_filter_expression = iceberg::filter::StringToFilter(filter.row);
    auto registry = std::make_shared<iceberg::filter::GandivaFunctionRegistry>(TimestampToTimestamptzShiftUs());

    bool must_apply_filter = !postfilter_on_gp;

    ARROW_ASSIGN_OR_RAISE(std::vector<int32_t> filter_field_ids, [&]() -> arrow::Result<std::vector<int32_t>> {
      std::vector<int32_t> result;

      iceberg::filter::SubtreeVisitor<iceberg::filter::ColumnExtractorVisitor> column_extractor;
      column_extractor.Visit(row_filter_expression);
      auto used_columns = column_extractor.GetResult();

      for (const auto& column_name : used_columns) {
        auto field_id = schema_->FindColumnIgnoreCase(column_name);
        if (field_id == std::nullopt) {
          return arrow::Status::ExecutionError("Column with name '", column_name, "' not found");
        }

        result.emplace_back(*field_id);
      }

      return result;
    }());

    row_filter_ = std::make_shared<iceberg::filter::RowFilter>(row_filter_expression, logger_);
    ice_filter =
        std::make_shared<GandivaFilterApplier>(row_filter_, registry, logger_, must_apply_filter, filter_field_ids);
  }

  ReaderProperties reader_properties(config_);
  std::optional<FileReaderProviderWithProperties::AdaptiveBatchInfo> adaptive_batch_info;
  if (config_.features.use_adaptive_batch_size) {
    FileReaderProviderWithProperties::AdaptiveBatchInfo result;
    result.min_rows = config_.limits.adaptive_batch_min_rows;
    result.max_rows = config_.limits.adaptive_batch_max_rows;
    result.max_bytes_in_column = config_.limits.adaptive_batch_max_bytes_in_column;
    result.max_bytes_in_batch = config_.limits.adaptive_batch_max_bytes_in_batch;

    adaptive_batch_info = result;
  }

  std::shared_ptr<const iceberg::IFileReaderProvider> file_reader_provider =
      std::make_shared<FileReaderProviderWithProperties>(std::move(reader_properties), fs_provider_,
                                                         field_ids_to_retrieve, adaptive_batch_info);

  if (config_.limits.metadata_cache_size > 0) {
    file_reader_provider = std::make_shared<CachingFileReaderProvider>(std::move(file_reader_provider),
                                                                       config_.limits.metadata_cache_size);
  }

  stream_ = iceberg::IcebergScanBuilder::MakeIcebergStream(
      meta_stream, std::move(positional_deletes), std::move(equality_deletes), std::move(equality_delete_config),
      rg_filter, ice_filter, *schema_, std::move(field_ids_to_retrieve), file_reader_provider, std::nullopt, logger_);

  stream_ = std::make_shared<LowercaseRenamingStream>(stream_);
  return arrow::Status::OK();
}

void Reader::LogPotentialStatsFilter() {
  if (potential_row_group_filter_logged_) {
    return;
  }

  if (!row_filter_) {
    return;
  }

  potential_row_group_filter_logged_ = true;

  TEA_LOG("Potential row group filter: " + row_filter_->GetFilterString());

  if (config_.debug.test_stats) {
    debug::SendPotentialRowGroupFilter(row_filter_->GetFilterString());
  }
}

arrow::Result<std::optional<iceberg::BatchWithSelectionVector>> Reader::GetNextBatch() {
  if (!stream_) {
    return std::nullopt;
  }
  ScopedTimerTicks timer(stats_.fetch_duration);

  auto batch = stream_->ReadNext();
  if (!batch) {
    if (!at_least_one_matching_row_in_file_) {
      LogPotentialStatsFilter();
    }
    return std::nullopt;
  }

  if (config_.debug.test_gandiva_filter && row_filter_) {
    debug::SendGandivaFilter(row_filter_->GetFilterString());
  }

  if (batch->GetPath() != current_file_name_) {
    if (!at_least_one_matching_row_in_file_) {
      LogPotentialStatsFilter();
    }
    at_least_one_matching_row_in_file_ = false;

    current_file_name_ = batch->GetPath();
  }

  if (batch->GetSelectionVector().Size() != 0) {
    at_least_one_matching_row_in_file_ = true;
  }

  stats_.rows_read += batch->GetSelectionVector().Size();
  for (auto& column : columns_) {
    auto arrow_col = batch->GetRecordBatch()->GetColumnByName(column.gp_name);
    if (arrow_col) {
      if (!MatchArrowColumn(arrow_col->type(), column.gp_type, column.gp_type_mode)) {
        return arrow::Status::ExecutionError(
            "Column '", column.gp_name, "' of type '", FormatTypeWithTypeMode(column.gp_type, column.gp_type_mode),
            "' (oid = ", column.gp_type, ") cannot be matched with arrow type ", arrow_col->type()->ToString(),
            "; parquet file is '", batch->GetPath(), "'");
      }
    }
  }
  return iceberg::BatchWithSelectionVector(batch->GetRecordBatch(), std::move(batch->GetSelectionVector()));
}

std::tuple<ReaderStats, iceberg::PositionalDeleteStats, iceberg::EqualityDeleteStats, S3Stats> Reader::GetStats()
    const {
  stats_.read_duration = fs_stats_->wait_read_stats->GetTotalDuration().value_or(0);
  stats_.positional_delete_apply_duration = positional_delete_timer_.GetTotalDuration().value_or(0);
  stats_.equality_delete_apply_duration = equality_delete_timer_.GetTotalDuration().value_or(0);
  stats_.gandiva_filter_build_duration = filter_build_timer_.GetTotalDuration().value_or(0);
  stats_.gandiva_filter_apply_duration = filter_apply_timer_.GetTotalDuration().value_or(0);

  if (entries_stream_ && entries_stream_->GetMeta()) {
    entries_stream_->GetMeta()->UpdateMetrics(stats_);
  }
  // TODO(gmusya): retry counting is broken. Fix (possibly on tea_reader.cpp level)
  S3Stats s3_stats;
  {
    std::lock_guard lock(fs_stats_->s3_read_stats_lock_);
    s3_stats.bytes_read = fs_stats_->s3_stats.bytes_read;
    s3_stats.requests = fs_stats_->s3_stats.requests;
  }
  return std::make_tuple(stats_, positional_delete_stats_, equality_delete_stats_, s3_stats);
}
}  // namespace tea
