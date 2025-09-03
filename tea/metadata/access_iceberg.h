#pragma once

#include <memory>
#include <string>
#include <utility>

#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
#include "tea/observability/planner_stats.h"

namespace tea::meta::access {

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromIceberg(
    const Config& config, TableId table_id, iceberg::filter::NodePtr filter,
    std::shared_ptr<iceberg::IFileSystemProvider> fs_provider, int64_t timestamp_to_timestamptz_shift_us,
    iceberg::filter::NodePtr partition_pruning_filter);

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromIcebergWithLocation(
    iceberg::filter::NodePtr filter, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    const std::string& location, int64_t timestamp_to_timestamptz_shift_us,
    std::function<bool(iceberg::Schema& schema)> use_avro_reader_schema,
    iceberg::filter::NodePtr partition_pruning_filter);

std::string GetIcebergTableLocation(const Config& config, TableId table_id);

}  // namespace tea::meta::access
