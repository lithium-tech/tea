#include "tea/metadata/entries_stream_config.h"

namespace tea {

iceberg::ice_tea::ManifestEntryDeserializerConfig MakeScanDeserializerConfigWithFilter() {
  iceberg::ice_tea::ManifestEntryDeserializerConfig cfg;
  cfg.datafile_config.extract_column_sizes = false;
  cfg.datafile_config.extract_distinct_counts = false;
  return cfg;
}

iceberg::ice_tea::ManifestEntryDeserializerConfig MakeFullScanDeserializerConfig() {
  iceberg::ice_tea::ManifestEntryDeserializerConfig cfg = MakeScanDeserializerConfigWithFilter();
  cfg.datafile_config.extract_value_counts = false;
  cfg.datafile_config.extract_null_value_counts = false;
  cfg.datafile_config.extract_nan_value_counts = false;
  cfg.datafile_config.extract_lower_bounds = false;
  cfg.datafile_config.extract_upper_bounds = false;
  return cfg;
}

}  // namespace tea
