#include "tea/samovar/network_layer/samovar_client.h"

namespace tea::samovar {

void ISamovarClient::UpdateTTL(const std::vector<std::string>& objects, std::chrono::seconds ttl) {
  for (auto& obj : objects) {
    UpdateTTL(obj, ttl);
  }
}

void ISamovarClient::DeleteCells(const std::vector<std::string>& objects) {
  for (const auto& obj : objects) {
    DeleteCell(obj);
  }
}

}  // namespace tea::samovar
