#pragma once

#include <map>
#include <memory>
#include <string>

#include "arrow/result.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/tea_column_stats.h"
#include "iceberg/tea_scan.h"

#include "tea/common/reader_properties.h"

namespace tea::meta {

struct RelationSize {
  double rows;
  int width;
};

class Estimator {
 public:
#ifdef TEA_BUILD_STATS
  static arrow::Result<ColumnStats> GetIcebergColumnStats(const Config &config, TableId table_id,
                                                          const std::string &column_name);
#endif

  static arrow::Result<RelationSize> GetRelationSizeFromIceberg(
      const Config &config, TableId table_id, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider);

  static arrow::Result<RelationSize> GetRelationSizeFromDataFiles(
      const iceberg::ice_tea::ScanMetadata &metadata, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
      ReaderProperties props);

  static std::map<std::string, int64_t> GetTotalMetricsFromIceberg(
      const Config &config, TableId table_id, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider);
};

}  // namespace tea::meta
