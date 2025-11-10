#include "tea/samovar/single_queue_client.h"

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

#include "tea/compression/compression_registry.h"
#include "tea/observability/tea_log.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/planner.h"
#include "tea/samovar/proto/samovar.pb.h"
#include "tea/samovar/samovar_data_client.h"
#include "tea/samovar/utils.h"
#include "tea/util/measure.h"

namespace tea::samovar {

SingleQueueClient::SingleQueueClient(std::shared_ptr<ISamovarClient> client, std::shared_ptr<Batcher> batcher,
                                     std::chrono::seconds ttl_seconds, const std::string& queue_id, int segment_count,
                                     const std::string& compressor_name, int segment_id, SamovarRole role,
                                     const std::unordered_set<int>& working_segment,
                                     std::chrono::seconds ttl_utils_seconds, std::shared_ptr<IBackoff> sync_backoff,
                                     bool need_sync_on_init)
    : ISamovarDataClient(client, working_segment.empty() ? segment_count : working_segment.size(), ttl_seconds),
      client_(client),
      batcher_(batcher),
      ttl_seconds_(ttl_seconds),
      queue_id_(queue_id),
      compressor(compression::CompressorFactory().GetCompressor(compressor_name)),
      role_(role),
      working_segment_(working_segment.empty() || working_segment.contains(segment_id)),
      segment_id_(segment_id),
      ttl_utils_seconds_(ttl_utils_seconds),
      sync_backoff_(sync_backoff),
      need_sync_on_init_(need_sync_on_init) {}

std::optional<samovar::AnnotatedDataEntry> SingleQueueClient::GetNextDataEntry() {
  if (!working_segment_) {
    return std::nullopt;
  }
  if (!previous_task_.empty()) {
    client_->AddIntoSet(GetDoneCell(), previous_task_);
    client_->RemoveFromSet(GetProcessingCell(), previous_task_);

    client_->UpdateTTL(std::vector{GetProcessingCell(), GetDoneCell()}, ttl_utils_seconds_);

    previous_task_.clear();
  }

  client_->UpdateTTL(std::vector{queue_id_, GetMetadataCell()}, ttl_seconds_);

  auto result = batcher_->GetNextDataEntry(queue_id_);
  if (!result) {
    OnProcessingEnd(GetCheckpointCell(), {queue_id_, GetMetadataCell(), GetCheckpointCell(), GetFileListCell()});
  }
  if (result) {
    result->set_gp_segment_executed(segment_id_);
    client_->AddIntoSet(GetProcessingCell(), result->SerializeAsString());
    client_->UpdateTTL(std::vector{GetProcessingCell(), GetDoneCell()}, ttl_utils_seconds_);
    previous_task_ = result->SerializeAsString();

    size_t file_index = result->data_entry().entry().file_index();
    if (file_index > 0) {
      auto* data_entry = result->mutable_data_entry();
      auto* entry = data_entry->mutable_entry();
      entry->set_file_path(GetFileList().filenames()[file_index - 1]);
    }
  }
  return result;
}

const samovar::ScanMetadata& SingleQueueClient::GetPlannedMetadata() {
  if (cached_result_metadata.has_value()) {
    return cached_result_metadata.value();
  }

  SetProcessingStage(Stage::kReading);
  client_->IncreaseNumericCell(GetInitScanCell());
  client_->UpdateTTL(GetInitScanCell(), ttl_seconds_);

  if (need_sync_on_init_) {
    ScopedTimerTicks timer(total_sync_time_);

    DoWithRetries<std::monostate>(
        [&]() -> std::optional<std::monostate> {
          std::optional<int> result = client_->GetNumericCell(GetInitScanCell());
          if (result && result.value() == segment_count_) {
            return std::monostate{};
          }
          return std::nullopt;
        },
        sync_backoff_);
  }

  samovar::ScanMetadata result_metadata;
  auto response = DoWithRetries<std::string>([&]() { return client_->GetCell(GetMetadataCell()); }, sync_backoff_);
  result_metadata.ParseFromString(response);

  client_->UpdateTTL(std::vector{queue_id_, GetMetadataCell(), GetFileListCell(), GetCheckpointCell()}, ttl_seconds_);
  cached_result_metadata = std::move(result_metadata);
  return cached_result_metadata.value();
}

const samovar::FileList& SingleQueueClient::GetFileList() {
  if (!file_list) {
    file_list = samovar::FileList{};
    auto response = DoWithRetries<std::string>([&]() { return client_->GetCell(GetFileListCell()); }, sync_backoff_);
    compressor->Decompress(response);
    file_list->ParseFromString(response);
  }
  return file_list.value();
}

void SingleQueueClient::FillSessionQueue(samovar::ScanMetadata&& scan_metadata, const samovar::FileList& all_file_list,
                                         const std::vector<samovar::AnnotatedDataEntry>& additional_data_entries) {
  SetProcessingStage(Stage::kFilling);
  SendDataEntries(client_, additional_data_entries, queue_id_, ttl_seconds_);

  {
    auto serialized_metadata = ClearDataEntries(scan_metadata).SerializeAsString();
    auto serialized_file_list = all_file_list.SerializeAsString();

    file_list = all_file_list;
    compressor->Compress(serialized_file_list);

    client_->SetCell(GetMetadataCell(), serialized_metadata, ttl_seconds_);
    TEA_LOG("Set serialized data at cell " + GetMetadataCell() + " with data size " +
            std::to_string(serialized_metadata.size()));
    client_->SetCell(GetFileListCell(), serialized_file_list, ttl_seconds_);
    TEA_LOG("Set file list at cell " + GetFileListCell() + " with data size " +
            std::to_string(serialized_file_list.size()));

    client_->UpdateTTL(std::vector{queue_id_, GetMetadataCell(), GetFileListCell(), GetCheckpointCell()}, ttl_seconds_);
  }
  OnProcessingStart(GetCheckpointCell());
}

std::string SingleQueueClient::GetInitScanCell() {
  if (!init_scan_cell_) {
    init_scan_cell_ = init_scan_prefix + queue_id_;
  }
  return *init_scan_cell_;
}

std::string SingleQueueClient::GetCheckpointCell() {
  if (!checkpoint_cell_) {
    checkpoint_cell_ = checkpoint_prefix + queue_id_;
  }
  return *checkpoint_cell_;
}

std::string SingleQueueClient::GetMetadataCell() {
  if (!metadata_cell_) {
    metadata_cell_ = metadata_prefix + queue_id_;
  }
  return *metadata_cell_;
}

std::string SingleQueueClient::GetFileListCell() {
  if (!file_list_cell_) {
    file_list_cell_ = file_list_prefix + queue_id_;
  }
  return *file_list_cell_;
}

int64_t SingleQueueClient::GetMetricValue(SamovarMetrics metric) const {
  switch (metric) {
    case SamovarMetrics::kResponseTime: {
      return working_segment_ ? client_->GetTotalResponseDurationTicks() : 0;
    }
    case SamovarMetrics::kRequestCount: {
      return working_segment_ ? client_->GetRequestCount() : 0;
    }
    case SamovarMetrics::kErrorsCount: {
      return working_segment_ ? client_->GetErrorsCount() : 0;
    }
    case SamovarMetrics::kSyncTime: {
      return total_sync_time_;
    }
    default:
      throw std::runtime_error("Unknown metric");
  }
}

std::string SingleQueueClient::GetProcessingCell() { return processing_queue_prefix + queue_id_; }
std::string SingleQueueClient::GetDoneCell() { return done_queue_prefix + queue_id_; }

SingleQueueClient::~SingleQueueClient() {
  if (!working_segment_) {
    return;
  }
  if (role_ != SamovarRole::kCoordinator) {
    try {
      OnProcessingEnd(GetCheckpointCell(), {queue_id_, GetMetadataCell(), GetCheckpointCell(), GetFileListCell()});
    } catch (...) {
      TEA_LOG("Can not clear redis - it will be dirty.");
    }
  }
}

}  // namespace tea::samovar
