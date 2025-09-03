#include "tea/metadata/access_empty.h"

#include <memory>

#include "iceberg/tea_scan.h"

namespace tea::meta::access {

iceberg::ice_tea::ScanMetadata FromEmpty() {
  iceberg::ice_tea::ScanMetadata meta;
  meta.schema = std::make_shared<iceberg::Schema>(iceberg::Schema(0, {}));
  return meta;
}

}  // namespace tea::meta::access
