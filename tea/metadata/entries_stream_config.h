#pragma once

#include "iceberg/manifest_entry.h"

namespace tea {

iceberg::ice_tea::ManifestEntryDeserializerConfig MakeScanDeserializerConfigWithFilter();
iceberg::ice_tea::ManifestEntryDeserializerConfig MakeFullScanDeserializerConfig();

}  // namespace tea
