#include "tea/metadata/access_iceberg.h"

#include <arrow/filesystem/filesystem.h>
#include <iceberg/manifest_entry.h>
#include <iceberg/manifest_entry_stats_getter.h>
#include <iceberg/table_metadata.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/catalog.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/common/fs/filesystem_wrapper.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/stats_filter/stats_filter.h"
#include "iceberg/schema.h"
#include "iceberg/tea_hive_catalog.h"
#include "iceberg/tea_nessie_catalog.h"
#include "iceberg/tea_rest_catalog.h"
#include "iceberg/tea_scan.h"

#include "tea/common/cancel.h"
#include "tea/common/config.h"
#include "tea/common/iceberg_fs.h"
#include "tea/common/iceberg_stats_filter.h"
#include "tea/common/utils.h"
#include "tea/metadata/entries_stream_config.h"
#include "tea/observability/planner_stats.h"
#include "tea/observability/tea_log.h"
#include "tea/util/logger.h"
#include "tea/util/measure.h"

namespace tea::meta::access {

std::shared_ptr<iceberg::ice_tea::RemoteCatalog> GetCatalog(const Config& config) {
  if (!config.hms_catalog.hms_endpoints.empty()) {
    for (const auto& [host, port] : config.hms_catalog.hms_endpoints) {
      TEA_LOG("HMS " + host + ":" + std::to_string(port));
      try {
        return std::make_shared<iceberg::ice_tea::HiveCatalog>(host, port);
      } catch (...) {
        TEA_LOG("Failed to connect to HMS (" + host + ":" + std::to_string(port) + ")");
      }
    }
    throw std::runtime_error("No correct HMS endpoints for iceberg catalog were provided " +
                             std::to_string(config.catalog.hms_endpoints.size()));
  }
  switch (config.catalog.type) {
    case CatalogConfig::CatalogType::kHMS: {
      for (const auto& [host, port] : config.catalog.hms_endpoints) {
        TEA_LOG("HMS " + host + ":" + std::to_string(port));
        try {
          return std::make_shared<iceberg::ice_tea::HiveCatalog>(host, port);
        } catch (...) {
          TEA_LOG("Failed to connect to HMS (" + host + ":" + std::to_string(port) + ")");
        }
      }
      throw std::runtime_error("No correct HMS endpoints for iceberg catalog were provided " +
                               std::to_string(config.catalog.hms_endpoints.size()));
    }
    case CatalogConfig::CatalogType::kNessie: {
#if USE_NESSIE
      for (const auto& [host, port] : config.catalog.nessie_endpoints) {
        try {
          return std::make_shared<iceberg::ice_tea::NessieCatalog>(host, port);
        } catch (...) {
          TEA_LOG("Failed to connect to Nessie catalog (" + host + ":" + std::to_string(port) + ")");
        }
      }
#endif
      throw std::runtime_error("No correct Nessie endpoints for iceberg catalog were provided");
    }
#if USE_REST
    case CatalogConfig::CatalogType::kREST: {
      if (config.catalog.rest_url.empty()) {
        throw std::runtime_error("REST URL for iceberg catalog is not provided");
      }

      if (config.catalog.rest_warehouse_id.empty()) {
        throw std::runtime_error("Warehouse id for iceberg catalog is not provided");
      }

      return std::make_shared<iceberg::ice_tea::RESTCatalog>(config.catalog.rest_url, config.catalog.rest_warehouse_id);
    }
#endif
  }
  throw std::runtime_error("No any correct endpoint for iceberg catalog were provided");
}

std::string GetIcebergTableLocation(const Config& config, TableId table_id) {
  auto catalog = GetCatalog(config);

  iceberg::catalog::TableIdentifier table_ident{.db = table_id.db_name, .name = table_id.table_name};
  auto table = catalog->LoadTable(table_ident);
  if (!table) {
    throw arrow::Status::ExecutionError("Cannot open Iceberg table ", table_id.db_name, ".", table_id.table_name);
  }
  return table->Location();
}

namespace {
class EmptyIcebergStream : public iceberg::ice_tea::IcebergEntriesStream {
 public:
  std::optional<iceberg::ManifestEntry> ReadNext() override { return std::nullopt; }
};
}  // namespace

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromIcebergWithLocation(
    iceberg::filter::NodePtr filter, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    const std::string& location, int64_t timestamp_to_timestamptz_shift_us,
    std::function<bool(iceberg::Schema& schema)> use_avro_reader_schema,
    iceberg::filter::NodePtr partition_pruning_filter, const CancelToken& cancel_token, SnapshotRef snapshot_ref) {
  PlannerStats stats;
  std::optional<ScopedTimerTicks> timer = ScopedTimerTicks(stats.plan_duration);

  TEA_LOG("Snapshot file " + location);
  auto maybe_fs = fs_provider->GetFileSystem(location);
  if (!maybe_fs.ok()) {
    throw maybe_fs.status();
  }
  std::shared_ptr<arrow::fs::FileSystem> fs = maybe_fs.ValueUnsafe();
  auto metrics = std::make_shared<tea::IcebergMetrics>();
  fs = std::make_shared<IcebergLoggingFileSystem>(fs, metrics);

  auto logger = std::make_shared<Logger>();
  logger->SetHandler("metrics:plan:data_files",
                     [&](const Logger::Message& msg) { stats.data_files_planned += std::stoll(msg); });
  logger->SetHandler("metrics:plan:positional_files",
                     [&](const Logger::Message& msg) { stats.positional_files_planned += std::stoll(msg); });
  logger->SetHandler("metrics:plan:equality_files",
                     [&](const Logger::Message& msg) { stats.equality_files_planned += std::stoll(msg); });
  logger->SetHandler("metrics:plan:dangling_positional_files",
                     [&](const Logger::Message& msg) { stats.dangling_positional_files += std::stoll(msg); });
  logger->SetHandler("metrics:plan:dangling_deletion_vector_files",
                     [&](const Logger::Message& msg) { stats.dangling_deletion_vector_files += std::stoll(msg); });

  auto result = [&]() -> arrow::Result<iceberg::ice_tea::ScanMetadata> {
    ARROW_ASSIGN_OR_RAISE(auto data, iceberg::ice_tea::ReadFile(fs, location));
    std::shared_ptr<iceberg::TableMetadataV2> table_metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
    if (!table_metadata) {
      return arrow::Status::ExecutionError("GetScanMetadata: failed to parse metadata " + location);
    }

    std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> entries_stream;
    std::shared_ptr<iceberg::Schema> scan_schema;
    if (!ResolveSnapshotId(table_metadata, snapshot_ref).has_value()) {
      entries_stream = std::make_shared<EmptyIcebergStream>();
      scan_schema = std::make_shared<iceberg::Schema>(iceberg::Schema(0, {}));
    } else {
      auto schema = tea::GetSchemaForSnapshot(table_metadata, snapshot_ref);
      scan_schema = schema;

      std::shared_ptr<iceberg::filter::StatsFilter> partition_pruning_stats_filter;
      if (partition_pruning_filter) {
        partition_pruning_stats_filter = std::make_shared<iceberg::filter::StatsFilter>(
            partition_pruning_filter, iceberg::filter::StatsFilter::Settings{});
      }

      bool use_reader_schema = filter ? use_avro_reader_schema(*schema) : false;
      std::optional<std::string> maybe_manifest_list_path =
          tea::GetManifestListPathForSnapshot(table_metadata, snapshot_ref);
      if (!maybe_manifest_list_path.has_value()) {
        return arrow::Status::ExecutionError("Manifest list path not found for snapshot");
      }
      entries_stream = iceberg::ice_tea::AllEntriesStream::Make(
          fs, maybe_manifest_list_path.value(), use_reader_schema, table_metadata->partition_specs, schema,
          partition_pruning_stats_filter,
          filter ? MakeScanDeserializerConfigWithFilter() : MakeFullScanDeserializerConfig());
      entries_stream = std::make_shared<CancellingStream>(entries_stream, cancel_token);
      if (filter) {
        entries_stream =
            std::make_shared<FilteringEntriesStream>(entries_stream, filter, schema, timestamp_to_timestamptz_shift_us);
      }
    }

    return iceberg::ice_tea::GetScanMetadata(*entries_stream, *table_metadata, scan_schema, logger);
  }();

  if (result.ok()) {
    // stats.plan_duration is correct only after timer is destroyed
    timer.reset();

    stats.iceberg_fs_duration = metrics->filesystem_duration;
    stats.iceberg_bytes_read = metrics->bytes_read;
    stats.iceberg_files_read = metrics->files_opened;
    stats.iceberg_requests = metrics->requests;

    for (const auto& part : result.ValueUnsafe().partitions) {
      for (const auto& layer : part) {
        for (const auto& data_entry : layer.data_entries_) {
          if (data_entry.dv) {
            stats.deletion_vectors_planned++;
          }
        }
      }
    }

    return std::make_pair(result.MoveValueUnsafe(), std::move(stats));
  } else {
    throw result.status();
  }
}

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromIceberg(
    const Config& config, TableId table_id, iceberg::filter::NodePtr filter,
    std::shared_ptr<iceberg::IFileSystemProvider> fs_provider, int64_t timestamp_to_timestamptz_shift_us,
    iceberg::filter::NodePtr partition_pruning_filter, const CancelToken& cancel_token, SnapshotRef snapshot_ref) {
  PlannerStats stats;
  std::string table_metadata_location;
  {
    std::optional<ScopedTimerTicks> timer = ScopedTimerTicks(stats.plan_duration);

    table_metadata_location = GetIcebergTableLocation(config, table_id);
    ++stats.catalog_connections_established;
  }
  auto [meta, fs_stats] = FromIcebergWithLocation(
      filter, fs_provider, table_metadata_location, timestamp_to_timestamptz_shift_us,
      [&](const iceberg::Schema& schema) {
        return schema.Columns().size() >= config.features.use_avro_projection_minimum_columns;
      },
      partition_pruning_filter, cancel_token, snapshot_ref);
  stats.Combine(fs_stats);
  return {meta, stats};
}

}  // namespace tea::meta::access
