#include "tea/samovar/network_layer/samovar_client.h"

namespace tea::samovar {

void ISamovarClient::UpdateTTL(const std::vector<std::string>& objects, std::chrono::seconds ttl) {
  for (auto& obj : objects) {
    UpdateTTL(obj, ttl);
  }
}

}  // namespace tea::samovar
