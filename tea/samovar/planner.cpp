#include "tea/samovar/planner.h"

#include <iceberg/streams/iceberg/data_entries_meta_stream.h>

#include <algorithm>
#include <memory>
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

namespace {

std::shared_ptr<ISamovarDataClient> MakeSamovarDataClient(const Config& config, const std::string& queue_name,
                                                          int segment_id, int segment_count,
                                                          const std::string& compressor_name, SamovarRole role,
                                                          const CancelToken& cancel_token) {
  auto backoff = CreateBackoff(config.samovar_config, cancel_token, std::make_shared<StageLogger>());

  std::shared_ptr<ISamovarClient> samovar_client = std::make_shared<SamovarRedisClient>(
      config.samovar_config.endpoints, backoff, config.samovar_config.request_timeout,
      config.samovar_config.connection_timeout);
  auto batch_size_scheduler = std::make_shared<ConstantBatchSizeScheduler>(config.samovar_config.batch_size);
  auto batcher = std::make_shared<Batcher>(samovar_client, batch_size_scheduler);

  std::shared_ptr<ISamovarDataClient> samovar_data_client_;

  switch (config.samovar_config.balancer_type) {
    case BalancerType::kOneQueue: {
      samovar_data_client_ = std::make_shared<SingleQueueClient>(
          samovar_client, batcher, config.samovar_config.ttl_seconds, queue_name, segment_count, compressor_name,
          segment_id, role, config.samovar_config.work_segments, config.samovar_config.ttl_utils_seconds, backoff);
      break;
    }
    default:
      throw arrow::Status::ExecutionError("Unexpected balancer type: ",
                                          static_cast<int>(config.samovar_config.balancer_type));
  }

  return samovar_data_client_;
}

}  // namespace

arrow::Result<PlannerStats> FillSamovar(const Config& config, iceberg::ice_tea::ScanMetadata&& meta, int segment_id,
                                        int segment_count, const std::string& queue_name,
                                        const std::string& compressor_name, const CancelToken& cancel_token) {
  TEA_LOG("Filling queue " + queue_name);
  PlannerStats stats;
  std::optional<ScopedTimerTicks> timer = ScopedTimerTicks(stats.plan_duration);
  for (const auto& part : meta.partitions) {
    for (const auto& layer : part) {
      stats.samovar_initial_tasks_count += layer.data_entries_.size();
    }
  }

  auto samovar_data_client = MakeSamovarDataClient(config, queue_name, segment_id, segment_count, compressor_name,
                                                   SamovarRole::kCoordinator, cancel_token);

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
  samovar_data_client->FillSessionQueue(std::move(samovar_representation), split_result.file_list,
                                        split_result.data_entries);

  TEA_LOG("Queue filled " + queue_name);
  timer.reset();
  return stats;
}

namespace {
class SamovarMetadataScheduler final : public meta::IMetadataScheduler {
 public:
  SamovarMetadataScheduler(const Config& config, int segment_id, int segment_count, const std::string& queue_name,
                           const std::string& compressor_name, SamovarRole role, const CancelToken& cancel_token) {
    samovar_data_client_ =
        MakeSamovarDataClient(config, queue_name, segment_id, segment_count, compressor_name, role, cancel_token);
  }

  std::vector<iceberg::AnnotatedDataPath> GetNextMetadata(size_t num_data_files) override {
    std::vector<iceberg::AnnotatedDataPath> result;
    for (size_t i = 0; i < num_data_files; ++i) {
      if (is_end_) {
        break;
      }

      auto request_result = samovar_data_client_->GetNextDataEntry();
      if (!request_result) {
        is_end_ = true;
      } else {
        result.emplace_back(
            ConvertSamovarAnnotatedDataEntryToAnnotatedDataEntry(*request_result, samovar_data_client_->GetFileList()));
      }
    }

    return result;
  }

  void UpdateMetrics(ReaderStats& stats) override {
    stats.samovar_total_response_duration_ticks = GetMetric(SamovarMetrics::kResponseTime);
    stats.samovar_requests_count = GetMetric(SamovarMetrics::kRequestCount);
    stats.samovar_errors_count = GetMetric(SamovarMetrics::kErrorsCount);
  }

  int64_t GetMetric(SamovarMetrics metric) { return samovar_data_client_->GetMetricValue(metric); }

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
  std::shared_ptr<ISamovarDataClient> samovar_data_client_;
  bool is_end_ = false;
  mutable std::optional<iceberg::ice_tea::ScanMetadata> total_scan_metadata_;
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

arrow::Result<std::pair<meta::PlannedMeta, PlannerStats>> FromSamovar(const Config& config, int segment_id,
                                                                      int segment_count, const std::string& queue_name,
                                                                      const std::string& compressor_name,
                                                                      const CancelToken& cancel_token) {
  if (config.samovar_config.wait_before_processing) {
    std::this_thread::sleep_for(config.samovar_config.time_before_processing_ms);
  }

  auto sched = std::make_shared<SamovarMetadataScheduler>(config, segment_id, segment_count, queue_name,
                                                          compressor_name, SamovarRole::kFollower, cancel_token);
  auto meta = meta::PlannedMeta(std::make_shared<meta::AnnotatedDataEntryStream>(sched), sched->GetPlannedMetadata());

  PlannerStats stats;
  return std::make_pair(std::move(meta), stats);
}

}  // namespace tea::samovar
