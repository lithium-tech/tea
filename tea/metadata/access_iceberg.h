#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
#include "tea/observability/planner_stats.h"
#include "tea/util/cancel.h"

namespace tea::meta::access {

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromIceberg(
    const Config& config, TableId table_id, iceberg::filter::NodePtr filter,
    std::shared_ptr<iceberg::IFileSystemProvider> fs_provider, int64_t timestamp_to_timestamptz_shift_us,
    iceberg::filter::NodePtr partition_pruning_filter, const CancelToken& cancel_token,
    SnapshotRef snapshot_ref = CurrentSnapshot{});

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromIcebergWithLocation(
    iceberg::filter::NodePtr filter, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    const std::string& location, int64_t timestamp_to_timestamptz_shift_us,
    std::function<bool(iceberg::Schema& schema)> use_avro_reader_schema,
    iceberg::filter::NodePtr partition_pruning_filter, const CancelToken& cancel_token,
    SnapshotRef snapshot_ref = CurrentSnapshot{});

std::string GetIcebergTableLocation(const Config& config, TableId table_id);

std::optional<std::string> GetMetadataProperty(const iceberg::TableMetadataV2& table_metadata,
                                               const std::string& property_name);

std::optional<std::string> GetSchemaNameMappingDefault(const iceberg::TableMetadataV2& table_metadata);

}  // namespace tea::meta::access
