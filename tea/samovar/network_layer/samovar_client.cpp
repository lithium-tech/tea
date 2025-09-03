#include "tea/samovar/network_layer/samovar_client.h"

namespace tea::samovar {

ISamovarClient::ISamovarClient(std::shared_ptr<IBackoff> backoff) : backoff_(backoff) {}

std::string ISamovarClient::GetCellWithRetries(const std::string& cell_name) {
  return DoWithRetries<std::string>([&]() -> std::optional<std::string> { return this->GetCell(cell_name); }, backoff_);
}

void ISamovarClient::UpdateTTL(const std::vector<std::string>& objects, std::chrono::seconds ttl) {
  for (auto& obj : objects) {
    UpdateTTL(obj, ttl);
  }
}

}  // namespace tea::samovar
