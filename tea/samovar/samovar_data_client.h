#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/proto/samovar.pb.h"

namespace tea::samovar {

enum class SamovarMetrics {
  kResponseTime,
  kRequestCount,
  kErrorsCount,
  kSyncTime,
};

enum class Stage {
  kPreparing,
  kFilling,
  kReading,
  kRemoving,
};

Stage GetProcessingStage();
void SetProcessingStage(Stage stage);

struct StageLogger : public IContextLogger {
  std::string GetLog() const override;
};

struct ISamovarDataClient {
 public:
  explicit ISamovarDataClient(std::shared_ptr<ISamovarClient> client, int segment_count,
                              std::chrono::seconds ttl_seconds);

  virtual std::optional<samovar::AnnotatedDataEntry> GetNextDataEntry() = 0;

  virtual const samovar::ScanMetadata& GetPlannedMetadata() = 0;
  virtual const samovar::FileList& GetFileList() = 0;

  virtual void FillSessionQueue(samovar::ScanMetadata&& scan_metadata, const samovar::FileList& file_list,
                                const std::vector<samovar::AnnotatedDataEntry>& additional_data_entries) = 0;

  virtual std::string GetQueueId() const = 0;

  virtual int64_t GetMetricValue(SamovarMetrics metric) const = 0;

  virtual ~ISamovarDataClient() = default;

 protected:
  void OnProcessingStart(const std::string& checkpoint_cell);
  void OnProcessingEnd(const std::string& checkpoint_cell, const std::vector<std::string>& cells_to_clear);

  std::shared_ptr<ISamovarClient> client_;
  int segment_count_;
  std::chrono::seconds ttl_seconds_;
  bool cleared_ = false;
  bool started_ = false;

  std::chrono::seconds cell_removing_time_seconds_;
};

}  // namespace tea::samovar
