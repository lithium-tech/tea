#include "tea/samovar/single_queue_client.h"

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

#include "tea/compression/compression_registry.h"
#include "tea/observability/tea_log.h"
#include "tea/samovar/network_layer/backoff.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/proto/samovar.pb.h"
#include "tea/samovar/utils.h"
#include "tea/util/measure.h"

namespace tea::samovar {

namespace {
void SyncSegments(std::shared_ptr<ISamovarClient> client, const std::string& cell_name, int segment_count,
                  std::shared_ptr<IBackoff> backoff, const std::string& msg) {
  DoWithRetries<std::monostate>(
      [&]() -> std::optional<std::monostate> {
        std::optional<int> result = client->GetNumericCell(cell_name);
        if (result && result.value() == segment_count) {
          return std::monostate{};
        }
        return std::nullopt;
      },
      backoff, msg);
}
}  // namespace

SingleQueueClient::SingleQueueClient(std::shared_ptr<ISamovarClient> client, std::shared_ptr<Batcher> batcher,
                                     std::chrono::seconds ttl_seconds, const std::string& queue_id, int segment_count,
                                     const std::string& compressor_name, SamovarRole role,
                                     std::shared_ptr<IBackoff> sync_backoff, std::shared_ptr<IBackoff> metadata_backoff,
                                     bool need_sync_on_init, uint32_t queue_push_batch_size)
    : client_(client),
      batcher_(batcher),
      ttl_seconds_(ttl_seconds),
      queue_id_(queue_id),
      compressor(compression::CompressorFactory().GetCompressor(compressor_name)),
      role_(role),
      metadata_backoff_(metadata_backoff),
      need_sync_on_init_(need_sync_on_init),
      sync_backoff_(sync_backoff),
      segment_count_(segment_count),
      queue_push_batch_size_(queue_push_batch_size) {
  // role semantics in context of SingleQueueClient class:
  // kCoordinator means that segment will write metadata
  // kFollower means that:
  // * segment will write write and read metadata
  // * or segment will read metadata
  if (role == SamovarRole::kFollower) {
    client_->IncreaseNumericCell(GetInitScanCell());
    client_->UpdateTTL(GetInitScanCell(), ttl_seconds_);
  }
}

void SingleQueueClient::WaitForManifestsQueue() {
  client_->IncreaseNumericCell(GetManifestsSyncScanCell());
  client_->UpdateTTL(GetManifestsSyncScanCell(), ttl_seconds_);

  ScopedTimerTicks timer(total_sync_time_);

  SyncSegments(client_, GetManifestsSyncScanCell(), segment_count_, metadata_backoff_, "wait_for_manifests_queue");
}

std::optional<samovar::AnnotatedDataEntry> SingleQueueClient::GetNextDataEntry() {
  client_->UpdateTTL(std::vector{queue_id_, GetMetadataCell()}, ttl_seconds_);

  auto result = batcher_->GetNextDataEntry(queue_id_);
  if (!result) {
    OnProcessingEnd(GetCheckpointCell(), {queue_id_, GetMetadataCell(), GetCheckpointCell(), GetFileListCell()});
  }

  if (result) {
    size_t file_index = result->data_entry().entry().file_index();
    if (file_index > 0) {
      auto* data_entry = result->mutable_data_entry();
      auto* entry = data_entry->mutable_entry();
      if (static_cast<int64_t>(file_index) > GetFileList().filenames().size()) {
        throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": file index is out of bounds (index is " +
                                 std::to_string(file_index) + ", size is " +
                                 std::to_string(GetFileList().filenames().size()) + ")");
      }
      entry->set_file_path(GetFileList().filenames()[file_index - 1]);
    }
  }
  return result;
}

std::optional<samovar::ManifestList> SingleQueueClient::GetNextManifest() {
  client_->UpdateTTL(std::vector{queue_id_, GetMetadataCell()}, ttl_seconds_);

  auto responses = client_->PopQueue(GetManifestCell(), 1);
  if (responses.empty()) {
    return std::nullopt;
  }
  if (responses.size() >= 2) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error");
  }
  samovar::ManifestList list;
  list.ParseFromString(responses.at(0));

  return list;
}

const samovar::ScanMetadata& SingleQueueClient::GetPlannedMetadata() {
  if (cached_result_metadata.has_value()) {
    return cached_result_metadata.value();
  }

  if (role_ == SamovarRole::kFollower && need_sync_on_init_) {
    ScopedTimerTicks timer(total_sync_time_);

    SyncSegments(client_, GetInitScanCell(), segment_count_, sync_backoff_, "sync_segments");
  }

  samovar::ScanMetadata result_metadata;
  auto response = DoWithRetries<std::string>([&]() { return client_->GetCell(GetMetadataCell()); }, metadata_backoff_,
                                             "wait_meta_from_coordinator");
  result_metadata.ParseFromString(response);

  cached_result_metadata = std::move(result_metadata);
  return cached_result_metadata.value();
}

const samovar::FileList& SingleQueueClient::GetFileList() {
  if (!file_list) {
    file_list = samovar::FileList{};
    // TODO(gmusya): seems redundant
    auto response = DoWithRetries<std::string>([&]() { return client_->GetCell(GetFileListCell()); }, metadata_backoff_,
                                               "wait_file_list_from_coordinator");
    compressor->Decompress(response);
    file_list->ParseFromString(response);
  }
  return file_list.value();
}

void SingleQueueClient::FillCommonInfo(samovar::ScanMetadata&& scan_metadata, samovar::FileList&& file_list_to_send) {
  auto serialized_metadata = ClearDataEntries(scan_metadata).SerializeAsString();
  auto serialized_file_list = file_list_to_send.SerializeAsString();

  compressor->Compress(serialized_file_list);

  client_->SetCell(GetMetadataCell(), serialized_metadata, ttl_seconds_);
  TEA_LOG("Set serialized data at cell " + GetMetadataCell() + " with data size " +
          std::to_string(serialized_metadata.size()));
  client_->SetCell(GetFileListCell(), serialized_file_list, ttl_seconds_);
  TEA_LOG("Set file list at cell " + GetFileListCell() + " with data size " +
          std::to_string(serialized_file_list.size()));
}

void SingleQueueClient::AppendToFilesQueue(std::vector<samovar::AnnotatedDataEntry>&& additional_data_entries) {
  SendDataEntries(client_, additional_data_entries, queue_id_, ttl_seconds_, queue_push_batch_size_);
}

void SingleQueueClient::FillFilesQueue(samovar::ScanMetadata&& scan_metadata, samovar::FileList&& all_file_list,
                                       std::vector<samovar::AnnotatedDataEntry>&& additional_data_entries) {
  AppendToFilesQueue(std::move(additional_data_entries));

  FillCommonInfo(ClearDataEntries(scan_metadata), std::move(all_file_list));

  client_->UpdateTTL(std::vector{queue_id_, GetMetadataCell(), GetFileListCell(), GetCheckpointCell()}, ttl_seconds_);
  OnProcessingStart(GetCheckpointCell());
}

void SingleQueueClient::FillManifestsQueue(samovar::ScanMetadata&& scan_metadata,
                                           const std::vector<samovar::ManifestList>& manifests) {
  SendManifestLists(client_, manifests, GetManifestCell(), ttl_seconds_, queue_push_batch_size_);

  scan_metadata.set_use_distributed_metadata_processing(true);

  FillCommonInfo(std::move(scan_metadata), samovar::FileList{});

  client_->UpdateTTL(std::vector{GetManifestCell(), GetMetadataCell(), GetFileListCell(), GetCheckpointCell()},
                     ttl_seconds_);
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

std::string SingleQueueClient::GetManifestsSyncScanCell() { return manifest_sync_prefix + queue_id_; }
std::string SingleQueueClient::GetManifestCell() { return manifest_queue_prefix + queue_id_; }

std::string SingleQueueClient::GetFileListCell() {
  if (!file_list_cell_) {
    file_list_cell_ = file_list_prefix + queue_id_;
  }
  return *file_list_cell_;
}

int64_t SingleQueueClient::GetMetricValue(SamovarMetrics metric) const {
  switch (metric) {
    case SamovarMetrics::kResponseTime: {
      return client_->GetTotalResponseDurationTicks();
    }
    case SamovarMetrics::kRequestCount: {
      return client_->GetRequestCount();
    }
    case SamovarMetrics::kErrorsCount: {
      return client_->GetErrorsCount();
    }
    case SamovarMetrics::kSyncTime: {
      return total_sync_time_;
    }
    default:
      throw std::runtime_error("Unknown metric");
  }
}

SingleQueueClient::~SingleQueueClient() {
  if (role_ != SamovarRole::kCoordinator) {
    try {
      OnProcessingEnd(GetCheckpointCell(), {queue_id_, GetMetadataCell(), GetCheckpointCell(), GetFileListCell()});
    } catch (...) {
      TEA_LOG("Can not clear redis - it will be dirty.");
    }
  }
}

void SingleQueueClient::OnProcessingStart(const std::string& checkpoint_cell) {
  started_ = true;
  client_->SetNumericCell(checkpoint_cell, segment_count_ + 1, ttl_seconds_);
}

void SingleQueueClient::OnProcessingEnd(const std::string& checkpoint_cell,
                                        const std::vector<std::string>& cells_to_clear) {
  if (cleared_ || !started_) {
    return;
  }
  cleared_ = true;
  std::optional<int> value_after_change = client_->DecreaseNumericCell(checkpoint_cell);
  if (!value_after_change) {
    TEA_LOG("Can not decrement cell, redis cluster will be unclear");
    return;
  }
  if (value_after_change == 1) {
    for (const auto& cell : cells_to_clear) {
      client_->DeleteCell(cell);
    }
  }

  if (value_after_change <= 0) {
    for (const auto& cell : cells_to_clear) {
      client_->DeleteCell(cell);
    }
    throw std::runtime_error("redis cluster state was flushed due query execution - checkpoint cell result mismatched");
  }
}

}  // namespace tea::samovar
