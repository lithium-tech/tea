#include "tea/samovar/planner.h"

#include <iceberg/common/fs/filesystem_provider.h>
#include <iceberg/streams/iceberg/data_entries_meta_stream.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/tea_scan.h"

#include "tea/common/cancel.h"
#include "tea/common/config.h"
#include "tea/common/iceberg_fs.h"
#include "tea/common/iceberg_stats_filter.h"
#include "tea/common/utils.h"
#include "tea/metadata/metadata.h"
#include "tea/metadata/planner.h"
#include "tea/observability/planner_stats.h"
#include "tea/observability/reader_stats.h"
#include "tea/observability/tea_log.h"
#include "tea/samovar/batcher.h"
#include "tea/samovar/network_layer/backoff.h"
#include "tea/samovar/network_layer/redis_client.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/proto/samovar.pb.h"
#include "tea/samovar/single_queue_client.h"
#include "tea/samovar/utils.h"
#include "tea/test_utils/common.h"
#include "tea/util/cancel.h"
#include "tea/util/measure.h"

namespace tea::samovar {

std::shared_ptr<SingleQueueClient> MakeSamovarDataClient(const SamovarConfig& config, const std::string& queue_name,
                                                         int segment_id, int segment_count, SamovarRole role,
                                                         const CancelToken& cancel_token) {
  auto sync_backoff = CreateBackoff(config.sync_backoff, cancel_token);
  auto metadata_backoff = CreateBackoff(config.metadata_backoff, cancel_token);

  std::shared_ptr<ISamovarClient> samovar_client =
      std::make_shared<SamovarRedisClient>(config.endpoints, config.request_timeout, config.connection_timeout);
  auto batch_size_scheduler = std::make_shared<ConstantBatchSizeScheduler>(config.batch_size);
  auto batcher = std::make_shared<Batcher>(samovar_client, batch_size_scheduler);

  std::shared_ptr<SingleQueueClient> samovar_data_client_;

  switch (config.balancer_type) {
    case BalancerType::kOneQueue: {
      samovar_data_client_ = std::make_shared<SingleQueueClient>(
          samovar_client, batcher, config.ttl_seconds, queue_name, segment_count, config.compressor_name, role,
          sync_backoff, metadata_backoff, config.need_sync_on_init, config.queue_push_batch_size);
      break;
    }
    default:
      throw arrow::Status::ExecutionError("Unexpected balancer type: ", static_cast<int>(config.balancer_type));
  }

  return samovar_data_client_;
}

arrow::Result<PlannerStats> FillSamovarWithManifests(const Config& config, std::shared_ptr<iceberg::Schema> schema,
                                                     std::deque<iceberg::ManifestFile> manifests, int segment_count,
                                                     std::shared_ptr<SingleQueueClient> samovar_client) {
  PlannerStats stats;
  std::optional<ScopedTimerTicks> timer = ScopedTimerTicks(stats.plan_duration);

  samovar::ScanMetadata result;
  *result.mutable_schema() = IcebergSchemaToTeapotSchema(schema);

  std::vector<samovar::ManifestList> samovar_manifests = ConvertToSamovarManifestLists(manifests);

  samovar_client->FillManifestsQueue(std::move(result), samovar_manifests);

  timer.reset();
  return stats;
}

arrow::Result<PlannerStats> FillSamovar(const Config& config, iceberg::ice_tea::ScanMetadata&& meta, int segment_count,
                                        std::shared_ptr<SingleQueueClient> samovar_client) {
  PlannerStats stats;
  std::optional<ScopedTimerTicks> timer = ScopedTimerTicks(stats.plan_duration);
  for (const auto& part : meta.partitions) {
    for (const auto& layer : part) {
      stats.samovar_initial_tasks_count += layer.data_entries_.size();
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto split_result, SplitPartitions(meta.partitions, config));

  stats.samovar_splitted_tasks_count = split_result.data_entries.size();
  auto samovar_representation = ConvertIcebergScanMetaToSamovarRepresentation(std::move(meta), split_result.file_list);

  std::sort(split_result.data_entries.begin(), split_result.data_entries.end(),
            [&](const samovar::AnnotatedDataEntry& lhs, const samovar::AnnotatedDataEntry& rhs) {
              if (lhs.partition_id() != rhs.partition_id()) {
                return lhs.partition_id() < rhs.partition_id();
              }
              if (lhs.data_entry().entry().file_index() != rhs.data_entry().entry().file_index()) {
                return lhs.data_entry().entry().file_index() < rhs.data_entry().entry().file_index();
              }
              const auto& lhs_segments = lhs.data_entry().segments();
              const auto& rhs_segments = rhs.data_entry().segments();
              if (lhs_segments.empty() || rhs_segments.empty()) {
                return false;
              }
              return lhs_segments[0].offset() < rhs_segments[0].offset();
            });

  if (config.samovar_config.allow_static_balancing &&
      static_cast<int>(split_result.data_entries.size()) <= segment_count) {
    // each segment can read one task and immediately close the connection
    for (auto& elem : split_result.data_entries) {
      elem.set_last_task_for_segment(true);
    }
  }

  samovar_client->FillFilesQueue(std::move(samovar_representation), std::move(split_result.file_list),
                                 std::move(split_result.data_entries));

  timer.reset();
  return stats;
}

namespace {
struct EmptyMetadataScheduler final : public meta::IMetadataScheduler {
  std::vector<iceberg::AnnotatedDataPath> GetNextMetadata(size_t num_data_files) override { return {}; };

  void UpdateMetrics(ReaderStats& stats) override {}
};

class SamovarMetadataScheduler final : public meta::IMetadataScheduler {
 public:
  SamovarMetadataScheduler(const Config& config, std::shared_ptr<SingleQueueClient> samovar_data_client)
      : samovar_data_client_(samovar_data_client),
        enable_static_balancing_(config.samovar_config.allow_static_balancing) {}

  std::vector<iceberg::AnnotatedDataPath> GetNextMetadata(size_t num_data_files) override {
    std::vector<iceberg::AnnotatedDataPath> result;
    for (size_t i = 0; i < num_data_files; ++i) {
      if (is_end_) {
        break;
      }

      auto request_result = samovar_data_client_->GetNextDataEntry();
      if (!request_result) {
        CloseConnection();
      } else {
        result.emplace_back(
            ConvertSamovarAnnotatedDataEntryToAnnotatedDataEntry(*request_result, samovar_data_client_->GetFileList()));
        if (enable_static_balancing_ && request_result->last_task_for_segment()) {
          samovar_data_client_->OnStaticBalancingProcessingEnd();
          CloseConnection();
        }
      }
    }

    return result;
  }

  void CloseConnection() {
    SaveMetrics();

    is_end_ = true;
    samovar_data_client_.reset();
  }

  void UpdateMetrics(ReaderStats& stats) override {
    if (samovar_data_client_) {
      SaveMetrics();
    }

    stats.samovar_total_response_duration_ticks = metrics.total_response_duration_ticks;
    stats.samovar_requests_count = metrics.request_count;
    stats.samovar_errors_count = metrics.error_count;
    stats.samovar_sync_duration = metrics.sync_duration;
  }

  int64_t GetMetric(SamovarMetrics metric) {
    iceberg::Ensure(samovar_data_client_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": internal error");

    return samovar_data_client_->GetMetricValue(metric);
  }

 private:
  void SaveMetrics() {
    iceberg::Ensure(samovar_data_client_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": internal error");

    metrics.total_response_duration_ticks = GetMetric(SamovarMetrics::kResponseTime);
    metrics.request_count = GetMetric(SamovarMetrics::kRequestCount);
    metrics.error_count = GetMetric(SamovarMetrics::kErrorsCount);
    metrics.sync_duration = GetMetric(SamovarMetrics::kSyncTime);
  }

  std::shared_ptr<SingleQueueClient> samovar_data_client_;
  bool is_end_ = false;
  const bool enable_static_balancing_ = true;

  struct SchedulerMetrics {
    int64_t total_response_duration_ticks = 0;
    int64_t request_count = 0;
    int64_t error_count = 0;
    DurationTicks sync_duration = 0;
  };

  SchedulerMetrics metrics;
};

}  // namespace

int GetCoordinator(const std::string& session_id, const TableSource& table_source, int segment_count) {
  std::hash<std::string> hasher;

  std::string table_id;
  {
    if (const auto* teapot_table = std::get_if<TeapotTable>(&table_source)) {
      table_id = teapot_table->table_id.ToString();
    } else if (const auto* file_table = std::get_if<tea::FileTable>(&table_source)) {
      table_id = file_table->url;
    } else if (const auto* iceberg_table = std::get_if<tea::IcebergTable>(&table_source)) {
      table_id = iceberg_table->table_id.ToString();
    }
  }

  return (hasher(table_id) ^ hasher(session_id)) % segment_count;
}

static std::chrono::milliseconds CalculateSleepTime(std::chrono::milliseconds min_time,
                                                    std::chrono::milliseconds max_time, const std::string& queue_name,
                                                    int segment_id) {
  std::mt19937 rnd(std::accumulate(queue_name.begin(), queue_name.end(), 0) + segment_id);

  std::uniform_int_distribution<int64_t> gen(min_time.count(), max_time.count());

  return std::chrono::milliseconds(gen(rnd));
}

static std::vector<samovar::AnnotatedDataEntry> ConvertToDataEntries(const iceberg::ManifestEntry& entry) {
  std::vector<samovar::AnnotatedDataEntry> data_files_to_process;

  constexpr uint64_t kLengthUntillEndOfFile = 0;

  std::vector<std::pair<uint64_t, uint64_t>> group_info;
  if (!entry.data_file.split_offsets.empty()) {
    const auto& offsets = entry.data_file.split_offsets;
    group_info.reserve(offsets.empty());
    for (size_t i = 0; i < offsets.size(); ++i) {
      uint64_t first_byte = offsets[i];
      uint64_t length = i + 1 == offsets.size() ? kLengthUntillEndOfFile : offsets[i + 1] - offsets[i];

      group_info.emplace_back(first_byte, length);
    }
  } else {
    constexpr uint64_t kParquetMagicBytesCount = 4;
    group_info = {{kParquetMagicBytesCount, kLengthUntillEndOfFile}};
  }

  for (const auto& [first_byte, length] : group_info) {
    samovar::AnnotatedDataEntry data_file_info;
    data_file_info.set_layer_id(0);
    data_file_info.set_partition_id(0);
    data_file_info.mutable_data_entry()->mutable_entry()->set_file_path(entry.data_file.file_path);

    auto segment = data_file_info.mutable_data_entry()->add_segments();
    segment->set_length(length);
    segment->set_offset(first_byte);

    data_files_to_process.push_back(std::move(data_file_info));
  }

  return data_files_to_process;
}

static std::vector<samovar::AnnotatedDataEntry> GetDataEntries(
    std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> stream, PlannerStats& stats) {
  std::vector<samovar::AnnotatedDataEntry> result;

  while (true) {
    std::optional<iceberg::ManifestEntry> maybe_entry = stream->ReadNext();
    if (!maybe_entry) {
      break;
    }
    if (maybe_entry->status == iceberg::ManifestEntry::Status::kDeleted) {
      continue;
    }

    ++stats.samovar_initial_tasks_count;

    std::vector<samovar::AnnotatedDataEntry> current_entries_to_process = ConvertToDataEntries(maybe_entry.value());
    result.insert(result.end(), current_entries_to_process.begin(), current_entries_to_process.end());
  }

  return result;
}

static arrow::Result<std::vector<samovar::AnnotatedDataEntry>> ManifestListToDataEntries(
    const samovar::ManifestList& manifest_list, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    std::shared_ptr<IcebergMetrics> metrics, PlannerStats& stats, const CancelToken& cancel_token,
    iceberg::filter::NodePtr filter_expr, std::shared_ptr<iceberg::Schema> schema,
    int64_t timestamp_to_timestamptz_shift_us) {
  const std::string& file_path = manifest_list.file_path();

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::fs::FileSystem> fs, fs_provider->GetFileSystem(file_path));
  fs = std::make_shared<IcebergLoggingFileSystem>(fs, metrics);

  ARROW_ASSIGN_OR_RAISE(std::string content, iceberg::ice_tea::ReadFile(fs, file_path));

  iceberg::ice_tea::ManifestEntryDeserializerConfig cfg;
  cfg.datafile_config.extract_partition_tuple = false;
  std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> entries =
      iceberg::ice_tea::make::ManifestEntriesStream(content, {}, cfg, true, false);
  entries = std::make_shared<tea::CancellingStream>(entries, cancel_token);
  if (filter_expr) {
    entries =
        std::make_shared<tea::FilteringEntriesStream>(entries, filter_expr, schema, timestamp_to_timestamptz_shift_us);
  }

  return GetDataEntries(entries, stats);
}

static arrow::Result<std::vector<samovar::AnnotatedDataEntry>> ManifestsToDataEntries(
    std::shared_ptr<SingleQueueClient> samovar_client, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    std::shared_ptr<IcebergMetrics> metrics, PlannerStats& stats, const CancelToken& cancel_token,
    iceberg::filter::NodePtr filter_expr, std::shared_ptr<iceberg::Schema> schema,
    int64_t timestamp_to_timestamptz_shift_us) {
  std::vector<samovar::AnnotatedDataEntry> result;
  while (true) {
    std::optional<samovar::ManifestList> maybe_next_manifest = samovar_client->GetNextManifest();
    if (!maybe_next_manifest.has_value()) {
      break;
    }

    const samovar::ManifestList& next_manifest = maybe_next_manifest.value();
    ARROW_ASSIGN_OR_RAISE(std::vector<samovar::AnnotatedDataEntry> data_files_to_process,
                          ManifestListToDataEntries(next_manifest, fs_provider, metrics, stats, cancel_token,
                                                    filter_expr, schema, timestamp_to_timestamptz_shift_us));

    result.insert(result.end(), data_files_to_process.begin(), data_files_to_process.end());
  }

  return result;
}

arrow::Result<std::pair<meta::PlannedMeta, PlannerStats>> FromSamovar(
    const Config& config, int segment_id, const std::string& queue_name, bool is_metadata_already_written,
    std::shared_ptr<SingleQueueClient> samovar_client, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    const CancelToken& cancel_token, iceberg::filter::NodePtr filter_expr, int64_t timestamp_to_timestamptz_shift_us) {
  // Followers should wait for some time (at least 3x the average s3 request latency) since no progress is
  // impossible until the coordinator writes the metadata to Samovar.
  // The coordinator does not have to wait because the metadata has already been written by him.
  if (config.samovar_config.wait_before_processing && !is_metadata_already_written) {
    std::chrono::milliseconds sleep_time =
        CalculateSleepTime(config.samovar_config.min_time_before_processing_ms,
                           config.samovar_config.max_time_before_processing_ms, queue_name, segment_id);

    std::this_thread::sleep_for(sleep_time);
  }

  auto response = samovar_client->GetPlannedMetadata();
  if (response.scan_already_finished()) {
    iceberg::ice_tea::ScanMetadata metadata;
    metadata.schema = TeapotSchemaToIcebergSchema(response.schema());

    auto sched = std::make_shared<EmptyMetadataScheduler>();

    auto meta = meta::PlannedMeta(std::make_shared<meta::AnnotatedDataEntryStream>(sched), std::move(metadata));
    return std::make_pair(std::move(meta), PlannerStats{});
  }

  PlannerStats stats;
  std::optional<ScopedTimerTicks> timer = ScopedTimerTicks(stats.plan_duration);

  if (response.use_distributed_metadata_processing()) {
    auto metrics = std::make_shared<IcebergMetrics>();

    std::shared_ptr<iceberg::Schema> schema = TeapotSchemaToIcebergSchema(response.schema());

    // read and parse some manifests
    ARROW_ASSIGN_OR_RAISE(std::vector<samovar::AnnotatedDataEntry> result,
                          ManifestsToDataEntries(samovar_client, fs_provider, metrics, stats, cancel_token, filter_expr,
                                                 schema, timestamp_to_timestamptz_shift_us));

    if (!result.empty()) {
      // write tasks to the entries queue
      stats.samovar_splitted_tasks_count += result.size();
      samovar_client->AppendToFilesQueue(std::move(result));
    }

    // wait for each segment to write its tasks to the entries queue
    samovar_client->WaitForManifestsQueue();

    iceberg::ice_tea::ScanMetadata metadata;
    metadata.schema = TeapotSchemaToIcebergSchema(response.schema());

    // process tasks from the entries queue
    auto sched = std::make_shared<SamovarMetadataScheduler>(config, samovar_client);
    auto meta = meta::PlannedMeta(std::make_shared<meta::AnnotatedDataEntryStream>(sched), std::move(metadata));

    timer.reset();

    UpdatePlannerStats(stats, *metrics);
    return std::make_pair(std::move(meta), stats);
  } else {
    auto response_file_list = samovar_client->GetFileList();

    auto total_scan_metadata = ConvertSamovarRepresentationToScanMeta(response, response_file_list);

    auto sched = std::make_shared<SamovarMetadataScheduler>(config, samovar_client);
    auto meta =
        meta::PlannedMeta(std::make_shared<meta::AnnotatedDataEntryStream>(sched), std::move(total_scan_metadata));

    return std::make_pair(std::move(meta), stats);
  }
}

}  // namespace tea::samovar
