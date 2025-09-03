#pragma once

#include <iceberg/tea_scan.h>

#include <string>

namespace tea {

struct ScanMetadataMessage {
  iceberg::ice_tea::ScanMetadata scan_metadata;
  std::string scan_metadata_identifier;
};

std::string ScanMetadataToJSONString(ScanMetadataMessage&& scan_metadata);
ScanMetadataMessage JSONStringToScanMetadata(const std::string& data);

}  // namespace tea
