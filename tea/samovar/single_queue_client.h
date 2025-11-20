#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "tea/compression/compressor.h"
#include "tea/samovar/batcher.h"
#include "tea/samovar/network_layer/backoff.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/proto/samovar.pb.h"
#include "tea/util/measure.h"

namespace tea::samovar {

enum class SamovarMetrics {
  kResponseTime,
  kRequestCount,
  kErrorsCount,
  kSyncTime,
};

enum class SamovarRole {
  kCoordinator,
  kFollower,
};

class SingleQueueClient {
 public:
  explicit SingleQueueClient(std::shared_ptr<ISamovarClient> client, std::shared_ptr<Batcher> batcher,
                             std::chrono::seconds ttl_seconds, const std::string& queue_id, int segment_count,
                             const std::string& compressor_name, SamovarRole role,
                             std::shared_ptr<IBackoff> sync_backoff, std::shared_ptr<IBackoff> metadata_backoff,
                             bool need_sync_on_init);

  std::optional<samovar::AnnotatedDataEntry> GetNextDataEntry();

  const samovar::ScanMetadata& GetPlannedMetadata();
  const samovar::FileList& GetFileList();

  void FillSessionQueue(samovar::ScanMetadata&& scan_metadata, const samovar::FileList& file_list,
                        const std::vector<samovar::AnnotatedDataEntry>& additional_data_entries);

  std::string GetQueueId() const { return queue_id_; }

  int64_t GetMetricValue(SamovarMetrics metric) const;

  ~SingleQueueClient();

  void OnProcessingStart(const std::string& checkpoint_cell);
  void OnProcessingEnd(const std::string& checkpoint_cell, const std::vector<std::string>& cells_to_clear);

 private:
  mutable std::shared_ptr<ISamovarClient> client_;
  std::shared_ptr<Batcher> batcher_;
  std::chrono::seconds ttl_seconds_;

  std::string queue_id_;

  static constexpr const char* metadata_prefix = "/samovar_meta";
  static constexpr const char* file_list_prefix = "/file_list";
  static constexpr const char* init_scan_prefix = "/init_scan";
  static constexpr const char* checkpoint_prefix = "/checkpoint";

  std::optional<std::string> init_scan_cell_;
  std::optional<std::string> checkpoint_cell_;
  std::optional<std::string> metadata_cell_;
  std::optional<std::string> file_list_cell_;

  std::optional<samovar::ScanMetadata> cached_result_metadata;
  std::optional<samovar::FileList> file_list;

  std::string GetInitScanCell();
  std::string GetCheckpointCell();
  std::string GetMetadataCell();
  std::string GetFileListCell();

  compression::CompressorPtr compressor;
  SamovarRole role_;

  std::shared_ptr<IBackoff> metadata_backoff_;

  DurationTicks total_sync_time_ = 0;

  int segment_count_ = 0;
  bool cleared_ = false;
  bool started_ = false;
};

}  // namespace tea::samovar
