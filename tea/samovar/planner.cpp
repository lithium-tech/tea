#include "tea/samovar/planner.h"

#include <iceberg/streams/iceberg/data_entries_meta_stream.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
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
#include "tea/samovar/samovar_data_client.h"
#include "tea/samovar/single_queue_client.h"
#include "tea/samovar/utils.h"
#include "tea/util/measure.h"

namespace tea::samovar {

std::shared_ptr<ISamovarDataClient> MakeSamovarDataClient(const SamovarConfig& config, const std::string& queue_name,
                                                          int segment_id, int segment_count, SamovarRole role,
                                                          const CancelToken& cancel_token) {
  auto backoff = CreateBackoff(config, cancel_token);

  std::shared_ptr<ISamovarClient> samovar_client =
      std::make_shared<SamovarRedisClient>(config.endpoints, config.request_timeout, config.connection_timeout);
  auto batch_size_scheduler = std::make_shared<ConstantBatchSizeScheduler>(config.batch_size);
  auto batcher = std::make_shared<Batcher>(samovar_client, batch_size_scheduler);

  std::shared_ptr<ISamovarDataClient> samovar_data_client_;

  switch (config.balancer_type) {
    case BalancerType::kOneQueue: {
      samovar_data_client_ = std::make_shared<SingleQueueClient>(
          samovar_client, batcher, config.ttl_seconds, queue_name, segment_count, config.compressor_name, segment_id,
          role, config.work_segments, config.ttl_utils_seconds, backoff, config.need_sync_on_init);
      break;
    }
    default:
      throw arrow::Status::ExecutionError("Unexpected balancer type: ", static_cast<int>(config.balancer_type));
  }

  return samovar_data_client_;
}

arrow::Result<PlannerStats> FillSamovar(const Config& config, iceberg::ice_tea::ScanMetadata&& meta, int segment_count,
                                        std::shared_ptr<ISamovarDataClient> samovar_client) {
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

  samovar_client->FillSessionQueue(std::move(samovar_representation), split_result.file_list,
                                   split_result.data_entries);

  timer.reset();
  return stats;
}

namespace {
class SamovarMetadataScheduler final : public meta::IMetadataScheduler {
 public:
  SamovarMetadataScheduler(const Config& config, std::shared_ptr<ISamovarDataClient> samovar_data_client)
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

  const iceberg::ice_tea::ScanMetadata& GetPlannedMetadata() const {
    if (total_scan_metadata_.has_value()) {
      return total_scan_metadata_.value();
    }

    auto response = samovar_data_client_->GetPlannedMetadata();
    auto response_file_list = samovar_data_client_->GetFileList();
    total_scan_metadata_ = ConvertSamovarRepresentationToScanMeta(response, response_file_list);
    return total_scan_metadata_.value();
  }

 private:
  void SaveMetrics() {
    iceberg::Ensure(samovar_data_client_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": internal error");

    metrics.total_response_duration_ticks = GetMetric(SamovarMetrics::kResponseTime);
    metrics.request_count = GetMetric(SamovarMetrics::kRequestCount);
    metrics.error_count = GetMetric(SamovarMetrics::kErrorsCount);
    metrics.sync_duration = GetMetric(SamovarMetrics::kSyncTime);
  }

  std::shared_ptr<ISamovarDataClient> samovar_data_client_;
  bool is_end_ = false;
  const bool enable_static_balancing_ = true;
  mutable std::optional<iceberg::ice_tea::ScanMetadata> total_scan_metadata_;

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

arrow::Result<std::pair<meta::PlannedMeta, PlannerStats>> FromSamovar(
    const Config& config, int segment_id, const std::string& queue_name, bool is_metadata_already_written,
    std::shared_ptr<ISamovarDataClient> samovar_client) {
  // Followers should wait for some time (at least 3x the average s3 request latency) since no progress is
  // impossible until the coordinator writes the metadata to Samovar.
  // The coordinator does not have to wait because the metadata has already been written by him.
  if (config.samovar_config.wait_before_processing && !is_metadata_already_written) {
    std::chrono::milliseconds sleep_time =
        CalculateSleepTime(config.samovar_config.min_time_before_processing_ms,
                           config.samovar_config.max_time_before_processing_ms, queue_name, segment_id);

    std::this_thread::sleep_for(sleep_time);
  }

  // Note that we always pass SamovarRole::kFollower to SamovarMetadataScheduler because from this point on, every
  // segment behaves as a follower.
  // Moreover, it is not recommended to pass SamovarRole::kCoordinator as this will prevent early metadata cleanup (see
  // SingleQueueClient destructor).
  // TODO(gmusya): the current interfaces seem to be error prone and need to be improved.
  auto sched = std::make_shared<SamovarMetadataScheduler>(config, samovar_client);
  auto meta = meta::PlannedMeta(std::make_shared<meta::AnnotatedDataEntryStream>(sched), sched->GetPlannedMetadata());

  PlannerStats stats;
  return std::make_pair(std::move(meta), stats);
}

}  // namespace tea::samovar
