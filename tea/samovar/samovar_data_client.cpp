#include "tea/samovar/samovar_data_client.h"

#include <stdexcept>

#include "tea/observability/tea_log.h"

namespace tea::samovar {

ISamovarDataClient::ISamovarDataClient(std::shared_ptr<ISamovarClient> client, int segment_count,
                                       std::chrono::seconds ttl_seconds)
    : client_(client), segment_count_(segment_count), ttl_seconds_(ttl_seconds) {}

void ISamovarDataClient::OnProcessingStart(const std::string& checkpoint_cell) {
  started_ = true;
  client_->SetNumericCell(checkpoint_cell, segment_count_ + 1, ttl_seconds_);
}

void ISamovarDataClient::OnProcessingEnd(const std::string& checkpoint_cell,
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
