#include "tea/gpext/tea_reader.h"

#include <arrow/array/array_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <iceberg/common/error.h>
#include <iceberg/common/selection_vector.h>
#include <iceberg/filter/representation/function.h>
#include <iceberg/filter/representation/node.h>
#include <iceberg/filter/representation/value.h>
#include <iceberg/result.h>
#include <iceberg/schema.h>

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/common/batch.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/filter/representation/serializer.h"
#include "iceberg/tea_scan.h"
#include "iceberg/uuid.h"

#include "tea/common/config.h"
#include "tea/common/iceberg_json.h"
#include "tea/common/reader_properties.h"
#include "tea/common/utils.h"
#include "tea/debug/stats_to_proto.h"
#include "tea/filter/teapot_file_filter/filter.h"
#include "tea/metadata/access_empty.h"
#include "tea/metadata/access_file.h"
#include "tea/metadata/access_iceberg.h"
#include "tea/metadata/access_teapot.h"
#include "tea/metadata/estimator.h"
#include "tea/metadata/planner.h"
#include "tea/observability/planner_stats.h"
#include "tea/observability/reader_stats.h"
#include "tea/observability/tea_log.h"
#include "tea/reader.h"
#include "tea/samovar/planner.h"
#include "tea/samovar/utils.h"
#include "tea/table/converter.h"
#include "tea/table/filter_convert.h"
#include "tea/table/shared_state.h"
#include "tea/util/measure.h"
#include "tea/util/signal_blocker.h"
#include "tea/util/thread_pool.h"

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "cdb/cdbvars.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
}

namespace tea {
namespace {

constexpr std::size_t kMaxErrorChars = 1023;

char error_message[kMaxErrorChars + 1] = {0};
bool is_call_successful;

void SetError(std::string_view message) {
  auto len = std::min(message.size(), kMaxErrorChars);
  std::memcpy(error_message, message.data(), len);
  error_message[len] = '\0';
  is_call_successful = false;
}

void SetArrowError(const arrow::Status &status) { SetError(status.message()); }

template <typename T>
void SetArrowError(const arrow::Result<T> &result) {
  SetArrowError(result.status());
}

// TODO(hvintus): this function is called with C++ non-trivial object on stack (string temporaries) and calls
// palloc, that could do long jumps. This could at very least leak memory. Replace with other ways of passing
// data.
char *StringToPostgres(const std::string_view s) {
  char *text = (char *)palloc(s.size() + 1);
  std::memcpy(text, s.data(), s.size());
  text[s.size()] = '\0';
  return text;
}

template <typename F>
void Invoke(F &&func) noexcept {
  try {
    std::invoke(std::forward<F>(func));
  } catch (const ::parquet::ParquetStatusException &e) {
    tea::SetArrowError(e.status());
  } catch (const ::parquet::ParquetException &e) {
    tea::SetArrowError(::arrow::Status::IOError(e.what()));
  } catch (const ::arrow::Status &e) {
    tea::SetArrowError(e);
  } catch (const ::std::exception &e) {
    tea::SetError(e.what());
  } catch (...) {
    tea::SetError("Unexpected exception.");
  }
}

static tea::ThreadPool *thread_pool = nullptr;

class BatchGetter {
 public:
  enum class Policy { kBlockingMainThread, kBlockingHelperThread, kPrefetchingHelperThread };
  explicit BatchGetter(Policy policy) : policy_(policy) {}

  arrow::Result<std::optional<iceberg::BatchWithSelectionVector>> GetBatch(tea::Reader &reader) {
    if (policy_ == Policy::kBlockingMainThread) {
      return reader.GetNextBatch();
    }

    if (policy_ == Policy::kBlockingHelperThread) {
      arrow::Result<std::optional<iceberg::BatchWithSelectionVector>> res;

      tea::thread_pool->Invoke([&]() { res = reader.GetNextBatch(); });
      return res;
    }

    if (policy_ == Policy::kPrefetchingHelperThread) {
      if (!task_.has_value()) {
        FetchBatch(reader);
      }

      return GetBatch();
    }

    return arrow::Status::ExecutionError("BatchGetter: unexpected policy ", static_cast<int>(policy_));
  }

  void MaybePrefetch(tea::Reader &reader) {
    if (policy_ == Policy::kPrefetchingHelperThread) {
      FetchBatch(reader);
    }
  }

  ~BatchGetter() {
    if (task_.has_value()) {
      if (task_->valid()) {
        task_->wait();
      }
    }
  }

 private:
  const Policy policy_;

  using TaskResult = arrow::Result<std::optional<iceberg::BatchWithSelectionVector>>;

  std::optional<std::future<TaskResult>> task_;

  void FetchBatch(tea::Reader &reader) {
    task_ = tea::thread_pool->Submit([&reader] { return reader.GetNextBatch(); });
  }

  TaskResult GetBatch() {
    auto result = (*std::move(task_)).get();
    task_.reset();
    return result;
  }
};

struct InternalContext {
  TableConfig table_config;

  std::shared_ptr<Reader> reader;
  std::shared_ptr<iceberg::IFileSystemProvider> fs_provider;
  std::shared_ptr<ArrowToGpConverter> converter;
  std::shared_ptr<TimerTicks> timer_ticks;
  std::shared_ptr<TimerClock> timer_clock;
  std::shared_ptr<BatchGetter> batch_getter;

  PlannerStats planner_stats;

  int32_t scan_identifier = 0;
  std::string session_id = "";

  ~InternalContext() {
    // get::BatchGetter with prefetch policy depends on get::Reader
    batch_getter.reset();

    // other objects do not depend on each other, so they can be deleted in any order
  }
};

void PrintLogs() {
  auto logs = GetLogs();
  for (const auto &msg : logs) {
    elog(LOG, "%s", msg.c_str());  // TODO(gmusya): this function may not return, causing memory leak. Fix
  }
}

arrow::Result<std::string> ReadFile(std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
                                    const std::string &path) {
  ARROW_ASSIGN_OR_RAISE(auto fs, fs_provider->GetFileSystem(path));
  return iceberg::ice_tea::ReadFile(fs, path);
}

}  // namespace
}  // namespace tea

#define TEA_RETURN_ARROW_NOT_OK(status_or_value_expr)                                  \
  do {                                                                                 \
    if (const auto &status_or_value = (status_or_value_expr); !status_or_value.ok()) { \
      tea::SetArrowError(status_or_value);                                             \
      return;                                                                          \
    }                                                                                  \
  } while (0)

#define TEA_INVOKE(log_level, fn)                             \
  do {                                                        \
    tea::is_call_successful = true;                           \
    tea::Invoke(fn);                                          \
    tea::PrintLogs();                                         \
    if (!tea::is_call_successful) {                           \
      elog((log_level), "Tea error: %s", tea::error_message); \
    }                                                         \
  } while (0)

#define TEA_INVOKE_WO_PRINT_LOGS(log_level, fn)               \
  do {                                                        \
    tea::is_call_successful = true;                           \
    tea::Invoke(fn);                                          \
    if (!tea::is_call_successful) {                           \
      elog((log_level), "Tea error: %s", tea::error_message); \
    }                                                         \
  } while (0)

#define TEA_INVOKE_IN_HELPER_THREAD(log_level, fn)            \
  do {                                                        \
    tea::is_call_successful = true;                           \
    if (tea::thread_pool) {                                   \
      tea::thread_pool->Invoke([&]() { tea::Invoke(fn); });   \
    } else {                                                  \
      tea::Invoke(fn);                                        \
    }                                                         \
    tea::PrintLogs();                                         \
    if (!tea::is_call_successful) {                           \
      elog((log_level), "Tea error: %s", tea::error_message); \
    }                                                         \
  } while (0)

namespace get {

auto ContextPtr(const TeaContextPtr context) { return static_cast<tea::InternalContext *>(context->ctx); }
auto &Context(const TeaContextPtr context) { return *static_cast<tea::InternalContext *>(context->ctx); }

auto Reader(const TeaContextPtr context) { return Context(context).reader; }
auto Converter(const TeaContextPtr context) { return Context(context).converter; }
auto TimerTicks(const TeaContextPtr context) { return Context(context).timer_ticks; }
auto TimerClock(const TeaContextPtr context) { return Context(context).timer_clock; }
auto ExtStats(const TeaContextPtr context) { return &context->ext_stats; }
auto BatchGetter(const TeaContextPtr context) { return Context(context).batch_getter; }
tea::PlannerStats &PlannerStats(const TeaContextPtr context) { return Context(context).planner_stats; }
auto FileSystemProvider(const TeaContextPtr context) { return Context(context).fs_provider; }
int ScanIdentifier(const TeaContextPtr context) { return Context(context).scan_identifier; }
std::string &SessionId(const TeaContextPtr context) { return Context(context).session_id; }
const tea::TableConfig &TableConfig(const TeaContextPtr context) { return Context(context).table_config; }
const tea::Config &Config(const TeaContextPtr context) { return TableConfig(context).config; }
const tea::SamovarConfig &SamovarConfig(const TeaContextPtr context) { return Config(context).samovar_config; }
const tea::TableSource &Source(const TeaContextPtr context) { return Context(context).table_config.source; }

}  // namespace get

std::shared_ptr<iceberg::IFileSystemProvider> MakeFileSystemProvider(const tea::Config &config) {
  iceberg::S3FileSystemGetter::Config s3_input_cfg;
  s3_input_cfg.access_key = config.s3.access_key;
  s3_input_cfg.secret_key = config.s3.secret_key;
  s3_input_cfg.region = config.s3.region;
  s3_input_cfg.scheme = config.s3.scheme;
  s3_input_cfg.endpoint_override = config.s3.endpoint_override;

  s3_input_cfg.connect_timeout = config.s3.connect_timeout;
  s3_input_cfg.request_timeout = config.s3.request_timeout;

  s3_input_cfg.retry_max_attempts = config.s3.retry_max_attempts;

  std::shared_ptr<iceberg::IFileSystemGetter> s3_fs_getter =
      std::make_shared<iceberg::S3FileSystemGetter>(s3_input_cfg);

  std::shared_ptr<iceberg::IFileSystemGetter> local_fs_getter = std::make_shared<iceberg::LocalFileSystemGetter>();

  if (config.debug.use_slow_filesystem) {
    s3_fs_getter =
        std::make_shared<iceberg::SlowFileSystemGetter>(s3_fs_getter, config.debug.slow_filesystem_delay_seconds);
    local_fs_getter =
        std::make_shared<iceberg::SlowFileSystemGetter>(local_fs_getter, config.debug.slow_filesystem_delay_seconds);
  }

  std::map<std::string, std::shared_ptr<iceberg::IFileSystemGetter>> schema_to_fs_builder = {
      {"file", local_fs_getter}, {"s3", s3_fs_getter}, {"s3a", s3_fs_getter}};

  return std::make_shared<iceberg::FileSystemProvider>(std::move(schema_to_fs_builder));
}

void UpdateConfig(const std::string &profile_to_tables_path, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
                  const std::string &table_url, tea::TableConfig &config) {
  arrow::Result<std::string> maybe_file_content = tea::ReadFile(fs_provider, profile_to_tables_path);
  if (!maybe_file_content.ok()) {
    TEA_LOG("Failed to read profile-to-tables config: " + maybe_file_content.status().message());
  } else {
    std::string file_content = maybe_file_content.MoveValueUnsafe();
    auto maybe_table_to_profile = tea::GetTableToProfileMapping(file_content);
    if (!maybe_table_to_profile.ok()) {
      TEA_LOG(maybe_table_to_profile.status().message());
    } else {
      std::unordered_map<std::string, std::string> table_to_profile = maybe_table_to_profile.MoveValueUnsafe();

      std::optional<std::string> table_id = [&]() -> std::optional<std::string> {
        const auto &source = config.source;
        if (std::holds_alternative<tea::TeapotTable>(source)) {
          return std::get<tea::TeapotTable>(source).table_id.ToString();
        }
        if (std::holds_alternative<tea::IcebergTable>(source)) {
          return std::get<tea::IcebergTable>(source).table_id.ToString();
        }
        return std::nullopt;
      }();
      if (table_id.has_value() && table_to_profile.contains(*table_id)) {
        TEA_LOG("Profile for table '" + *table_id + "' is overrided as " + table_to_profile.at(*table_id));
        config = tea::ConfigSource::GetTableConfig(table_url, table_to_profile.at(*table_id));
      }
    }
  }
}

void LogSourceType(const tea::TableSource &source) {
  if (auto *iceberg_table = std::get_if<tea::IcebergTable>(&source); iceberg_table != nullptr) {
    TEA_LOG("Iceberg table: " + iceberg_table->table_id.ToString());
  } else if (auto *teapot_table = std::get_if<tea::TeapotTable>(&source); teapot_table != nullptr) {
    TEA_LOG("Teapot table: " + teapot_table->table_id.ToString());
  } else if (auto *file_table = std::get_if<tea::FileTable>(&source); file_table != nullptr) {
    TEA_LOG("File table: " + file_table->url);
  } else if (auto *special_table = std::get_if<tea::EmptyTable>(&source); special_table != nullptr) {
    TEA_LOG("Empty table");
  } else if (auto *total_metrics_table = std::get_if<tea::IcebergMetricsTable>(&source);
             total_metrics_table != nullptr) {
    TEA_LOG("Total metrics table");
  } else {
    TEA_LOG("Unknown table type");
  }
}

TeaContextPtr TeaContextCreateUntracked(const char *url) {
  TeaContextPtr result = nullptr;
  TEA_INVOKE_IN_HELPER_THREAD(
      ERROR, ([url, &result]() {
        static uint32_t scan_identifier = 0;

        result = new TeaContext();

        auto internal_ctx = new tea::InternalContext();
        internal_ctx->scan_identifier = ++scan_identifier;
        internal_ctx->table_config = tea::ConfigSource::GetTableConfig(url);

        result->ctx = internal_ctx;

        internal_ctx->timer_clock = std::make_shared<tea::TimerClock>();
        internal_ctx->timer_ticks = std::make_shared<tea::TimerTicks>();

        const auto &cfg = internal_ctx->table_config.config;
        internal_ctx->fs_provider = MakeFileSystemProvider(cfg);

        const std::string &profile_to_tables_path = internal_ctx->table_config.config.profile_to_tables_path;
        if (!profile_to_tables_path.empty()) {
          UpdateConfig(profile_to_tables_path, internal_ctx->fs_provider, url, internal_ctx->table_config);
        }

        LogSourceType(internal_ctx->table_config.source);

        internal_ctx->reader = std::make_shared<tea::Reader>(cfg, internal_ctx->fs_provider);
        internal_ctx->converter = std::make_shared<tea::ArrowToGpConverter>();

        // TODO(gmusya): localize code with cp1251/utf-8 handling
        bool is_cp1251 = GetDatabaseEncoding() == PG_WIN1251;
        internal_ctx->converter->SetEncoding(is_cp1251 ? tea::ArrowToGpConverter::Encoding::kCp1251
                                                       : tea::ArrowToGpConverter::Encoding::kUtf8);

        if (is_cp1251) {
          internal_ctx->converter->SetNonAsciiSubstitutionParameters(cfg.json.substitute_unicode_if_json_type);
        }

        const auto batch_getter_policy = [&]() {
          if (!cfg.features.use_helper_thread || tea::thread_pool == nullptr) {
            return tea::BatchGetter::Policy::kBlockingMainThread;
          }
          if (cfg.features.prefetch) {
            return tea::BatchGetter::Policy::kPrefetchingHelperThread;
          }
          return tea::BatchGetter::Policy::kBlockingHelperThread;
        }();

        internal_ctx->batch_getter = std::make_shared<tea::BatchGetter>(batch_getter_policy);

        result->ext_stats.heap_form_tuple_ticks = 0;
        result->ext_stats.convert_duration_ticks = 0;
        result->ext_stats.is_logged = false;
      }));
  return result;
}

void TeaContextDestroyUntracked(TeaContextPtr tea_ctx) {
  TEA_INVOKE(WARNING, [=] {
    if (auto ctx = get::ContextPtr(tea_ctx)) {
      delete ctx;
    }
    delete tea_ctx;
  });
}

void TeaContextFinalize() {
  TEA_INVOKE_IN_HELPER_THREAD(WARNING, [] { TEA_RETURN_ARROW_NOT_OK(tea::Reader::Finalize()); });
  TEA_INVOKE(WARNING, [] {
    if (tea::thread_pool) {
      tea::thread_pool->Stop();
      delete tea::thread_pool;
      tea::thread_pool = nullptr;
    }
    tea::FinalizeLogger();
  });
}

void TeaContextInitialize(int db_encoding) {
  TEA_INVOKE(ERROR, [] {
    tea::InitializeLogger();
    auto config = tea::ConfigSource::GetConfig();
    if (config.features.use_helper_thread) {
      tea::SignalBlocker signal_blocker;
      tea::thread_pool = new tea::ThreadPool(1);
    }
  });
  TEA_INVOKE_IN_HELPER_THREAD(ERROR, [=] { TEA_RETURN_ARROW_NOT_OK(tea::Reader::Initialize(db_encoding)); });
}

static tea::TableType TableTypeFromSource(const tea::TableSource &source) {
  if (std::holds_alternative<tea::IcebergTable>(source)) {
    return tea::TableType::kIceberg;
  } else if (std::holds_alternative<tea::TeapotTable>(source)) {
    return tea::TableType::kTeapot;
  } else if (std::holds_alternative<tea::FileTable>(source)) {
    return tea::TableType::kFile;
  } else {
    return tea::TableType::kEmpty;
  }
}

static std::string CommonFilterToTeapotFileFilter(std::string serialized_filter) {
  return tea::filter::GetTeapotFileFilter(
      iceberg::filter::StringToFilter(serialized_filter),
      tea::filter::TeapotFileFilterContext{.timestamp_to_timestamptz_shift_us_ = tea::TimestampToTimestamptzShiftUs()});
}

// invoked on segment
void TeaContextPlanForeign(TeaContextPtr tea_ctx, const ForeignScanParams *params) {
  TEA_INVOKE_IN_HELPER_THREAD(ERROR, [=] {
    get::SessionId(tea_ctx) = params->session_id;

    auto reader = get::Reader(tea_ctx);
    reader->SetColumns(params->projection.columns, params->projection.columns + params->projection.ncolumns);
    get::Converter(tea_ctx)->SetColumnInfo(reader->columns());

    auto meta_message = tea::JSONStringToScanMetadata(params->metadata);

    auto maybe_plan_meta = [&]() {
      bool from_samovar = get::SamovarConfig(tea_ctx).turn_on_samovar;
      if (from_samovar) {
        return tea::samovar::FromSamovar(
            get::Config(tea_ctx), params->segment_id, params->segment_count, meta_message.scan_metadata_identifier,
            get::SamovarConfig(tea_ctx).compressor_name, tea::samovar::SamovarRole::kFollower);
      } else {
        tea::ScanMetadataMessage meta = std::move(meta_message);
        meta.scan_metadata =
            tea::SplitPartitionsAndFilter(std::move(meta.scan_metadata), params->segment_id, params->segment_count);
        return tea::meta::FromIcebergMetadata(std::move(meta.scan_metadata));
      }
    }();

    TEA_RETURN_ARROW_NOT_OK(maybe_plan_meta.status());
    auto plan_meta_with_stats = maybe_plan_meta.MoveValueUnsafe();
    get::PlannerStats(tea_ctx).Combine(plan_meta_with_stats.second);

    tea::Reader::SerializedFilter filter;
    filter.extracted = params->filter.all_extracted ? params->filter.all_extracted : "";
    filter.row = params->filter.row ? params->filter.row : "";
    TEA_RETURN_ARROW_NOT_OK(reader->Plan(std::move(plan_meta_with_stats.first), filter, true));
  });
}

static iceberg::ice_tea::ScanMetadata GetMetaFromTeapot(TeaContextPtr tea_ctx, const tea::TableConfig &table_config,
                                                        int segment_id, int segment_count,
                                                        const std::string &session_id,
                                                        const std::string &extracted_filter) {
  auto res_with_stats = tea::meta::access::FromTeapot(
      table_config.config, std::get<tea::TeapotTable>(table_config.source).table_id.ToString(), session_id, segment_id,
      segment_count, CommonFilterToTeapotFileFilter(extracted_filter));
  get::PlannerStats(tea_ctx).Combine(res_with_stats.second);
  return (std::move(res_with_stats.first));
}

static iceberg::ice_tea::ScanMetadata GetMetaFromIceberg(TeaContextPtr tea_ctx, const tea::TableConfig &table_config,
                                                         const std::string &extracted_filter) {
  auto filter = iceberg::filter::StringToFilter(extracted_filter);
  auto res_with_stats = tea::meta::access::FromIceberg(
      table_config.config, std::get<tea::IcebergTable>(table_config.source).table_id, filter,
      get::FileSystemProvider(tea_ctx), tea::TimestampToTimestamptzShiftUs(),
      table_config.config.features.use_iceberg_metadata_partition_pruning ? filter : nullptr);
  get::PlannerStats(tea_ctx).Combine(res_with_stats.second);
  return std::move(res_with_stats.first);
}

static iceberg::ice_tea::ScanMetadata GetMetadataForSegment(TeaContextPtr tea_ctx, const tea::TableConfig &table_config,
                                                            int segment_id, int segment_count,
                                                            const std::string &session_id,
                                                            const std::string &extracted_filter) {
  auto access_type = TableTypeFromSource(get::Source(tea_ctx));
  switch (access_type) {
    case tea::TableType::kIceberg:
      throw arrow::Status::ExecutionError("Combination external table + iceberg access + no samovar is not supported");
    case tea::TableType::kTeapot:
      return GetMetaFromTeapot(tea_ctx, table_config, segment_id, segment_count, session_id, extracted_filter);
    case tea::TableType::kFile: {
      if (segment_id == 0) {
        return tea::meta::access::FromFileUrl(std::get<tea::FileTable>(table_config.source).url,
                                              get::FileSystemProvider(tea_ctx));
      } else {
        return tea::meta::access::FromEmpty();
      }
    }
    case tea::TableType::kEmpty:
      return tea::meta::access::FromEmpty();
    default:
      throw arrow::Status::ExecutionError("Unexpected access type ", static_cast<int>(access_type));
  }
}

static iceberg::ice_tea::ScanMetadata GetAllMetadata(TeaContextPtr tea_ctx, const tea::TableConfig &table_config,
                                                     const std::string &session_id,
                                                     const std::string &extracted_filter) {
  auto access_type = TableTypeFromSource(get::Source(tea_ctx));
  if (access_type == tea::TableType::kIceberg) {
    return GetMetaFromIceberg(tea_ctx, table_config, extracted_filter);
  }
  return GetMetadataForSegment(tea_ctx, table_config, 0, 1, session_id, extracted_filter);
}

using ResultType = arrow::Result<std::pair<tea::meta::PlannedMeta, tea::PlannerStats>>;

inline std::string GetLocation(TeaContextPtr tea_ctx, const ExternalScanParams *params) {
  iceberg::filter::NodePtr filter =
      iceberg::filter::StringToFilter(params->filter.all_extracted ? params->filter.all_extracted : "");
  iceberg::Ensure(filter != nullptr, "IcebergMetricsTable: filter must be 'location = <location>'");
  iceberg::Ensure(filter->node_type == iceberg::filter::NodeType::kFunction,
                  "IcebergMetricsTable: filter must be 'location = <location>'");
  auto function_filter = std::static_pointer_cast<iceberg::filter::FunctionNode>(filter);

  iceberg::Ensure(function_filter->function_signature.function_id == iceberg::filter::FunctionID::kEqual,
                  "IcebergMetricsTable: filter must be 'location = <location>'");
  iceberg::Ensure(function_filter->arguments.size() == 2,
                  "IcebergMetricsTable: filter must be 'location = <location>'");

  {
    auto lhs_argument = function_filter->arguments.at(0);
    iceberg::Ensure(lhs_argument->node_type == iceberg::filter::NodeType::kVariable,
                    "IcebergMetricsTable: filter must be 'location = <location>'");
    auto lhs_value = std::static_pointer_cast<iceberg::filter::VariableNode>(lhs_argument);
    iceberg::Ensure(lhs_value->column_name == "location",
                    "IcebergMetricsTable: filter must be 'location = <location>'");
  }
  tea::TableId table_id;
  {
    auto rhs_argument = function_filter->arguments.at(1);
    iceberg::Ensure(rhs_argument->node_type == iceberg::filter::NodeType::kConst,
                    "IcebergMetricsTable: filter must be 'location = <location>'");

    auto rhs_value = std::static_pointer_cast<iceberg::filter::ConstNode>(rhs_argument);
    iceberg::Ensure(rhs_value->value.GetValueType() == iceberg::filter::ValueType::kString,
                    "IcebergMetricsTable: filter must be 'location = <location>'");

    return rhs_value->value.GetValue<iceberg::filter::ValueType::kString>();
  }
}

inline tea::TableId GetTableId(std::string location) {
  iceberg::Ensure(location.starts_with("tea://"), "IcebergMetricsTable: location must start with 'tea://'");

  const auto nested_url = location.substr(std::string("tea://").size());
  const auto components = iceberg::SplitUrl(nested_url);

  return tea::TableId::FromString(components.location);
}

void TeaContextPrepareTotalMetricsTable(TeaContextPtr tea_ctx, const ExternalScanParams *params) {
  if (params->segment_id != 0) {
    return;
  }
  std::string location = GetLocation(tea_ctx, params);
  tea::TableId table_id = GetTableId(location);
  TEA_LOG("IcebergMetricsTable: table id is " + table_id.ToString());

  arrow::FieldVector fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  {
    arrow::StringBuilder builder;
    iceberg::Ensure(builder.Append(location));
    std::shared_ptr<arrow::Array> array = iceberg::ValueSafe(builder.Finish());

    fields.emplace_back(std::make_shared<arrow::Field>("location", arrow::utf8()));
    arrays.emplace_back(array);
  }

  constexpr std::array<std::string_view, 6> kFields = {"total-records",          "total-data-files",
                                                       "total-files-size",       "total-equality-deletes",
                                                       "total-position-deletes", "total-delete-files"};

  auto metrics = tea::meta::Estimator::GetTotalMetricsFromIceberg(get::Config(tea_ctx), table_id,
                                                                  get::FileSystemProvider(tea_ctx));

  for (std::string_view field : kFields) {
    std::string str_field = std::string(field);
    arrow::Int64Builder builder;
    auto iter = metrics.find(str_field);
    if (iter != metrics.end()) {
      iceberg::Ensure(builder.Append(iter->second));
      TEA_LOG(std::string(field) + ", " + std::to_string(iter->second));
    } else {
      iceberg::Ensure(builder.AppendNull());
      TEA_LOG(std::string(field) + ", null");
    }

    std::shared_ptr<arrow::Array> array = iceberg::ValueSafe(builder.Finish());

    std::replace(str_field.begin(), str_field.end(), '-', '_');

    fields.emplace_back(std::make_shared<arrow::Field>(str_field, arrow::int64()));
    arrays.emplace_back(array);
  }

  auto schema = std::make_shared<arrow::Schema>(fields);

  auto batch = arrow::RecordBatch::Make(schema, 1, arrays);

  auto converter = get::Converter(tea_ctx);
  auto iceberg_batch = iceberg::BatchWithSelectionVector(batch, iceberg::SelectionVector<int32_t>(1));

  iceberg::Ensure(converter->Prepare(std::move(iceberg_batch)));
}

// invoked on segment
void TeaContextPlanExternal(TeaContextPtr tea_ctx, const ExternalScanParams *params) {
  TEA_INVOKE_IN_HELPER_THREAD(ERROR, [=] {
    get::SessionId(tea_ctx) = params->session_id;

    auto reader = get::Reader(tea_ctx);
    reader->SetColumns(params->projection.columns, params->projection.columns + params->projection.ncolumns);
    get::Converter(tea_ctx)->SetColumnInfo(reader->columns());

    tea::Reader::SerializedFilter filter;
    filter.extracted = params->filter.all_extracted ? params->filter.all_extracted : "";
    filter.row = params->filter.row ? params->filter.row : "";

    if (std::holds_alternative<tea::IcebergMetricsTable>(get::Source(tea_ctx))) {
      TeaContextPrepareTotalMetricsTable(tea_ctx, params);
      return;
    }

    auto make_samovar_queue_name = [&]() {
      return tea::samovar::MakeSessionIdentifier(get::Source(tea_ctx), get::SamovarConfig(tea_ctx).cluster_id,
                                                 get::SessionId(tea_ctx), "0", params->slice_id,
                                                 get::ScanIdentifier(tea_ctx), false);
    };

    const bool from_samovar = get::SamovarConfig(tea_ctx).turn_on_samovar;
    const auto target_coordinator =
        tea::samovar::GetCoordinator(get::SessionId(tea_ctx), get::Source(tea_ctx), params->segment_count);
    const bool is_coordinator = params->segment_id == target_coordinator;
    if (from_samovar) {
      TEA_LOG("Samovar coordinator for query is " + std::to_string(target_coordinator));
      if (is_coordinator) {
        TEA_LOG("I am samovar coordinator");
        iceberg::ice_tea::ScanMetadata all_meta =
            GetAllMetadata(tea_ctx, get::TableConfig(tea_ctx), get::SessionId(tea_ctx), filter.extracted);

        auto maybe_stats = tea::samovar::FillSamovar(get::Config(tea_ctx), std::move(all_meta), params->segment_id,
                                                     params->segment_count, make_samovar_queue_name(),
                                                     get::SamovarConfig(tea_ctx).compressor_name);
        TEA_RETURN_ARROW_NOT_OK(maybe_stats);
        get::PlannerStats(tea_ctx).Combine(maybe_stats.MoveValueUnsafe());
      }
    }

    auto maybe_plan_meta = [&]() -> ResultType {
      if (from_samovar) {
        return tea::samovar::FromSamovar(get::Config(tea_ctx), params->segment_id, params->segment_count,
                                         make_samovar_queue_name(), get::SamovarConfig(tea_ctx).compressor_name,
                                         tea::samovar::SamovarRole::kFollower);
      } else {
        auto meta_for_me = GetMetadataForSegment(tea_ctx, get::TableConfig(tea_ctx), params->segment_id,
                                                 params->segment_count, get::SessionId(tea_ctx), filter.extracted);
        return tea::meta::FromIcebergMetadata(std::move(meta_for_me));
      }
    }();

    TEA_RETURN_ARROW_NOT_OK(maybe_plan_meta.status());
    auto plan_meta_with_stats = maybe_plan_meta.MoveValueUnsafe();
    get::PlannerStats(tea_ctx).Combine(plan_meta_with_stats.second);
    TEA_RETURN_ARROW_NOT_OK(reader->Plan(std::move(plan_meta_with_stats.first), filter, true));
  });
}

// invoked on master
void TeaContextPlanAnalyze(TeaContextPtr tea_ctx, const AnalyzeParams *params) {
  TEA_INVOKE_IN_HELPER_THREAD(ERROR, [=] {
    get::SessionId(tea_ctx) = params->session_id;

    auto reader = get::Reader(tea_ctx);
    reader->SetColumns(params->projection.columns, params->projection.columns + params->projection.ncolumns);
    get::Converter(tea_ctx)->SetColumnInfo(reader->columns());

    auto all_meta = GetAllMetadata(tea_ctx, get::TableConfig(tea_ctx), get::SessionId(tea_ctx), "");

    auto maybe_plan_meta = tea::meta::FromIcebergMetadata(std::move(all_meta));
    TEA_RETURN_ARROW_NOT_OK(maybe_plan_meta.status());

    auto plan_meta_with_stats = maybe_plan_meta.MoveValueUnsafe();
    get::PlannerStats(tea_ctx).Combine(plan_meta_with_stats.second);
    TEA_RETURN_ARROW_NOT_OK(
        reader->Plan(std::move(plan_meta_with_stats.first), tea::Reader::SerializedFilter{}, false));
  });
}

void TeaContextPrepareTuple(TeaContextPtr tea_ctx, bool *has_row) {
  *has_row = false;

  auto converter = get::Converter(tea_ctx);

  // hot path
  if (converter->HasUnreadRows()) {
    *has_row = true;
    return;
  }

  // cold path, may create new threads
  TEA_INVOKE(ERROR, [=] {
    if (std::holds_alternative<tea::IcebergMetricsTable>(get::Source(tea_ctx))) {
      return;
    }

    // reader may return batch where all rows are filtered, so we need check batches in loop
    while (true) {
      auto batch_getter = get::BatchGetter(tea_ctx);
      auto reader = get::Reader(tea_ctx);

      auto maybe_next_batch = batch_getter->GetBatch(*reader);
      TEA_RETURN_ARROW_NOT_OK(maybe_next_batch);
      auto next_batch = maybe_next_batch.MoveValueUnsafe();
      if (!next_batch.has_value()) {
        // no more rows in scan
        return;
      }
      TEA_RETURN_ARROW_NOT_OK(converter->Prepare(std::move(next_batch.value())));
      if (converter->HasUnreadRows()) {
        *has_row = true;
        batch_getter->MaybePrefetch(*reader);
        return;
      }
    }
  });
}

void TeaContextSkipTuple(const TeaContextPtr tea_ctx) {
  TEA_INVOKE_WO_PRINT_LOGS(ERROR, [=] {
    auto converter = get::Converter(tea_ctx);
    auto status = converter->SkipRow();
    TEA_RETURN_ARROW_NOT_OK(status);
  });
}

void TeaContextFetchTuple(const TeaContextPtr tea_ctx, struct FmgrInfo *fcinfo, GpDatum *values, bool *isnull) {
  tea::ScopedTimerTicks timer(get::ExtStats(tea_ctx)->convert_duration_ticks);
  // hot path
  TEA_INVOKE_WO_PRINT_LOGS(ERROR, [=] {
    auto converter = get::Converter(tea_ctx);
    auto status = converter->ReadRow(values, isnull, tea::GetSharedState()->GetCharsetConverter(fcinfo));
    TEA_RETURN_ARROW_NOT_OK(status);
  });
}

void TeaContextGetRelationSize(TeaContextPtr tea_ctx, const char *session_id, const ReaderScanProjection *projection,
                               ReaderRelationSize *relsize) {
  TEA_INVOKE_IN_HELPER_THREAD(ERROR, [=] {
    tea::ReaderProperties reader_properties(get::Config(tea_ctx));
    auto fs_provider = get::FileSystemProvider(tea_ctx);

    bool is_iceberg = TableTypeFromSource(get::Source(tea_ctx)) == tea::TableType::kIceberg;
    arrow::Result<tea::meta::RelationSize> res = [&]() -> arrow::Result<tea::meta::RelationSize> {
      if (is_iceberg) {
        return tea::meta::Estimator::GetRelationSizeFromIceberg(
            get::Config(tea_ctx), std::get<tea::IcebergTable>(get::Source(tea_ctx)).table_id,
            get::FileSystemProvider(tea_ctx));
      } else {
        auto meta = GetAllMetadata(tea_ctx, get::TableConfig(tea_ctx), session_id, "");
        return tea::meta::Estimator::GetRelationSizeFromDataFiles(meta, fs_provider, reader_properties);
      }
    }();
    TEA_RETURN_ARROW_NOT_OK(res);
    relsize->rows = res->rows;
    relsize->width = res->width;
  });
}

#ifdef TEA_BUILD_STATS
// TODO(gmusya): semantic of this function is weird, fix it
void TeaContextGetIcebergColumnStats(const char *url, const char *session_id, const char *column_name,
                                     ColumnStats *result) {
  TEA_INVOKE_IN_HELPER_THREAD(ERROR, [=] {
    tea::TableConfig table_config = tea::ConfigSource::GetTableConfig(url);

    auto table_type = TableTypeFromConfig(table_config);
    if (table_type != tea::TableType::kTeapot && table_type != tea::TableType::kIceberg) {
      throw arrow::Status::ExecutionError("GetIcebergColumnStats supported only for teapot and iceberg tables");
    }

    bool is_iceberg = table_type == tea::TableType::kIceberg;

    const auto table_id = is_iceberg ? std::get<tea::IcebergTable>(table_config.source).table_id
                                     : std::get<tea::TeapotTable>(table_config.source).table_id;
    const auto res = tea::meta::Estimator::GetIcebergColumnStats(table_config.config, table_id, column_name);
    TEA_RETURN_ARROW_NOT_OK(res);
    *result = res.ValueUnsafe();
  });
}
#endif

static void SetOptions(ReaderOptions *options, const tea::Config &config) {
  options->use_custom_heap_form_tuple = config.features.use_custom_heap_form_tuple;
  options->ext_table_filter_walker_for_projection = config.features.ext_table_filter_walker_for_projection;
  options->use_virtual_tuple = config.features.use_virtual_tuple;
  options->postfilter_on_gp = config.features.postfilter_on_gp;

  options->ignored_exprs.func_exprs.data = config.features.filter_ignored_func_exprs.data();
  options->ignored_exprs.func_exprs.len = config.features.filter_ignored_func_exprs.size();

  options->ignored_exprs.op_exprs.data = config.features.filter_ignored_op_exprs.data();
  options->ignored_exprs.op_exprs.len = config.features.filter_ignored_op_exprs.size();
}

void TeaContextGetOptions(TeaContextPtr tea_ctx, ReaderOptions *options) {
  TEA_INVOKE_IN_HELPER_THREAD(ERROR, ([tea_ctx, options] {
                                const tea::Config &config = get::Config(tea_ctx);
                                SetOptions(options, config);
                              }));
}

// invoked on master
void TeaContextGetScanMetadata(const TeaContextPtr tea_ctx, const char *session_id, const char *file_filter,
                               char **metadata, int segment_count) {
  TEA_INVOKE_IN_HELPER_THREAD(
      ERROR, ([tea_ctx, session_id, &metadata, file_filter, segment_count] {
        iceberg::ice_tea::ScanMetadata all_meta =
            GetAllMetadata(tea_ctx, get::TableConfig(tea_ctx), session_id, file_filter ? file_filter : "");
        std::string serialized_meta;
        if (get::SamovarConfig(tea_ctx).turn_on_samovar) {
          iceberg::UuidGenerator gen;
          std::string queue_name =
              tea::samovar::MakeSessionIdentifier(get::Source(tea_ctx), get::SamovarConfig(tea_ctx).cluster_id,
                                                  session_id, gen.CreateRandom().ToString(), 1, 1, true);

          std::shared_ptr<iceberg::Schema> schema = all_meta.schema;

          auto stats = tea::samovar::FillSamovar(get::Config(tea_ctx), std::move(all_meta), 0, segment_count,
                                                 queue_name, get::SamovarConfig(tea_ctx).compressor_name);
          TEA_RETURN_ARROW_NOT_OK(stats);
          get::PlannerStats(tea_ctx).Combine(stats.ValueUnsafe());

          // TODO(gmusya): change tea::ScanMetadataMessage
          serialized_meta = tea::ScanMetadataToJSONString(tea::ScanMetadataMessage{
              .scan_metadata = iceberg::ice_tea::ScanMetadata{.schema = schema, .partitions = {}},
              .scan_metadata_identifier = queue_name});
        } else {
          serialized_meta = tea::ScanMetadataToJSONString(
              tea::ScanMetadataMessage{.scan_metadata = std::move(all_meta), .scan_metadata_identifier = ""});
        }

        *metadata = tea::StringToPostgres(serialized_meta);
      }));
}

void TeaContextLogStats(const TeaContextPtr tea_ctx, const char *event) {
  TEA_INVOKE_IN_HELPER_THREAD(
      ERROR, ([tea_ctx, event] {
        if (std::holds_alternative<tea::IcebergMetricsTable>(get::Source(tea_ctx))) {
          return;
        }

        if (!tea_ctx->ext_stats.is_logged) {
          tea_ctx->ext_stats.is_logged = true;
          auto reader = get::Reader(tea_ctx);

          constexpr int64_t kNanosInSecond = 1'000'000'000;

          const auto seconds_passed =
              static_cast<double>(std::chrono::nanoseconds(get::TimerClock(tea_ctx)->duration()).count()) /
              kNanosInSecond;
          const auto ticks_passed = get::TimerTicks(tea_ctx)->duration();
          double ticks_per_second = ticks_passed / seconds_passed;

          const auto [reader_stats, pos_del_stats, eq_del_stats, s3_stats] = reader->GetStats();
          const auto session_id = get::SessionId(tea_ctx);
          const auto scan_identifier = get::ScanIdentifier(tea_ctx);
          const std::string version = TEA_VERSION;

          tea::Log(tea::FormatStats(event ? event : "", session_id, scan_identifier, version, ticks_passed,
                                    ticks_per_second, get::PlannerStats(tea_ctx), reader_stats, s3_stats,
                                    tea_ctx->ext_stats, pos_del_stats, eq_del_stats));
          if (get::Config(tea_ctx).debug.test_stats) {
            tea::debug::SendStats(ticks_passed, ticks_per_second, get::PlannerStats(tea_ctx), reader_stats, s3_stats,
                                  tea_ctx->ext_stats, pos_del_stats, eq_del_stats, GpIdentity.segindex);
          }
        }
      }));
}
