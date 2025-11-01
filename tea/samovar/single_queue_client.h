#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "tea/compression/compressor.h"
#include "tea/samovar/batcher.h"
#include "tea/samovar/network_layer/backoff.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/planner.h"
#include "tea/samovar/proto/samovar.pb.h"
#include "tea/samovar/samovar_data_client.h"

namespace tea::samovar {

class SingleQueueClient : public ISamovarDataClient {
 public:
  explicit SingleQueueClient(std::shared_ptr<ISamovarClient> client, std::shared_ptr<Batcher> batcher,
                             std::chrono::seconds ttl_seconds, const std::string& queue_id, int segment_count,
                             const std::string& compressor_name, int segment_id, SamovarRole role,
                             const std::unordered_set<int>& working_segment, std::chrono::seconds ttl_utils_seconds,
                             std::shared_ptr<IBackoff> init_scan_backoff, bool need_sync_on_init);

  std::optional<samovar::AnnotatedDataEntry> GetNextDataEntry() override;

  const samovar::ScanMetadata& GetPlannedMetadata() override;
  const samovar::FileList& GetFileList() override;

  void FillSessionQueue(samovar::ScanMetadata&& scan_metadata, const samovar::FileList& file_list,
                        const std::vector<samovar::AnnotatedDataEntry>& additional_data_entries) override;

  std::string GetQueueId() const override { return queue_id_; }

  int64_t GetMetricValue(SamovarMetrics metric) const override;

  ~SingleQueueClient() override;

 private:
  mutable std::shared_ptr<ISamovarClient> client_;
  std::shared_ptr<Batcher> batcher_;
  std::chrono::seconds ttl_seconds_;

  std::string queue_id_;

  static constexpr const char* metadata_prefix = "/samovar_meta";
  static constexpr const char* file_list_prefix = "/file_list";
  static constexpr const char* init_scan_prefix = "/init_scan";
  static constexpr const char* checkpoint_prefix = "/checkpoint";
  static constexpr const char* processing_queue_prefix = "/processing";
  static constexpr const char* done_queue_prefix = "/done";

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
  std::string GetProcessingCell();
  std::string GetDoneCell();

  compression::CompressorPtr compressor;
  SamovarRole role_;

  bool working_segment_;
  std::string previous_task_;
  int segment_id_;

  std::chrono::seconds ttl_utils_seconds_;
  std::shared_ptr<IBackoff> sync_backoff_;

  const bool need_sync_on_init_ = true;
};

}  // namespace tea::samovar
