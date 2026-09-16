#include "tea/samovar/network_layer/samovar_client.h"

namespace tea::samovar {

void ISamovarClient::UpdateTTL(const std::vector<std::string>& objects, std::chrono::seconds ttl) {
  for (auto& obj : objects) {
    UpdateTTL(obj, ttl);
  }
}

int ISamovarClient::RegisterSegment(const std::string& query_scans_count_key,
                                    const std::vector<std::string>& cells_to_register, std::chrono::seconds ttl,
                                    bool check_query_scans) {
  int scans_count = 0;
  if (check_query_scans) {
    scans_count = IncreaseNumericCell(query_scans_count_key);
    UpdateTTL(query_scans_count_key, ttl);
  }
  for (const auto& cell : cells_to_register) {
    IncreaseNumericCell(cell);
    UpdateTTL(cell, ttl);
  }
  return scans_count;
}

void ISamovarClient::PublishData(const std::string& queue_name, const std::vector<std::string>& queue_elements,
                                 const std::vector<std::pair<std::string, std::string>>& cells_with_data,
                                 std::chrono::seconds ttl) {
  if (!queue_name.empty() && !queue_elements.empty()) {
    PushQueue(queue_name, queue_elements);
    UpdateTTL(queue_name, ttl);
  }
  for (const auto& [cell, data] : cells_with_data) {
    SetCell(cell, data, ttl);
  }
}

}  // namespace tea::samovar
