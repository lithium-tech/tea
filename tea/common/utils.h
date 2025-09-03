#pragma once

#include <parquet/metadata.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "iceberg/tea_scan.h"

#include "teapot/teapot.pb.h"

namespace tea {

teapot::Schema IcebergSchemaToTeapotSchema(const std::shared_ptr<iceberg::Schema>& schema);

std::shared_ptr<iceberg::Schema> TeapotSchemaToIcebergSchema(const teapot::Schema& schema);

iceberg::ice_tea::ScanMetadata MetadataResponseResultToScanMetadata(const teapot::MetadataResponseResult& meta);

iceberg::ice_tea::ScanMetadata SplitPartitionsAndFilter(iceberg::ice_tea::ScanMetadata&& scan_metadata,
                                                        const int segment_id, const int segment_count);

class DeletePlanner {
 public:
  // Tries to reduce number of partitions
  // Example:
  //   Input:
  //     Partition 1:
  //       Layer 1: data A, delete B
  //     Partition 2:
  //       Layer 1: data C, delete B, delete D
  //   ------------------------------
  //   Output:
  //     Partition 1:
  //       Layer 1: data C, delete D
  //       Layer 2: data A, delete B
  static iceberg::ice_tea::ScanMetadata OptimizeScanMetadata(iceberg::ice_tea::ScanMetadata&& meta);

  using DeleteId = uint32_t;
  using DeleteSet = std::set<DeleteId>;

  using SetId = uint32_t;
  using Chain = std::vector<SetId>;
  using ChainId = uint32_t;

  // Split all sets into chains. All sets in one chain are nested within each other
  // Example:
  //   Input:
  //     [{6, 4}, {7, 8, 9}, {6}, {7}, {7, 9}]
  //   Output:
  //     [[2, 0], [3, 1, 4]] (meaning {0} -> {0, 2} and {3} -> {3, 5} -> {3, 4, 5})
  static std::vector<Chain> GroupNestedDeletes(const std::vector<std::set<DeleteId>>& delete_sets);
};

}  // namespace tea
