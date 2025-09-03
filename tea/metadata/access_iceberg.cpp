#include "tea/metadata/access_iceberg.h"

#include <arrow/filesystem/filesystem.h>
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
#include "iceberg/tea_rest_catalog.h"
#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
#include "tea/metadata/entries_stream_config.h"
#include "tea/observability/planner_stats.h"
#include "tea/observability/tea_log.h"
#include "tea/util/measure.h"

namespace tea::meta::access {
namespace {

struct IcebergMetrics {
  uint64_t requests = 0;
  uint64_t bytes_read = 0;
  uint64_t files_opened = 0;

  DurationTicks filesystem_duration = 0;
};

class LoggingInputFile : public iceberg::InputFileWrapper {
 public:
  LoggingInputFile(std::shared_ptr<arrow::io::RandomAccessFile> file, std::shared_ptr<IcebergMetrics> metrics)
      : InputFileWrapper(file), metrics_(metrics) {}

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    ScopedTimerTicks timer(metrics_->filesystem_duration);
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::ReadAt(position, nbytes, out);
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    ScopedTimerTicks timer(metrics_->filesystem_duration);
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes, out);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ScopedTimerTicks timer(metrics_->filesystem_duration);
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes);
  }

 private:
  void TakeRequestIntoAccount(int64_t bytes) {
    ++metrics_->requests;
    metrics_->bytes_read += bytes;
  }

  std::shared_ptr<IcebergMetrics> metrics_;
};

class IcebergLoggingFileSystem : public iceberg::FileSystemWrapper {
 public:
  IcebergLoggingFileSystem(std::shared_ptr<arrow::fs::FileSystem> fs, std::shared_ptr<IcebergMetrics> metrics)
      : FileSystemWrapper(fs), metrics_(metrics) {}

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(const std::string& path) override {
    ++metrics_->files_opened;
    ARROW_ASSIGN_OR_RAISE(auto file, FileSystemWrapper::OpenInputFile(path));
    return std::make_shared<LoggingInputFile>(file, metrics_);
  }

 private:
  std::shared_ptr<IcebergMetrics> metrics_;
};
}  // namespace

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
    case CatalogConfig::CatalogType::kREST: {
#if USE_REST
      for (const auto& [host, port] : config.catalog.rest_endpoints) {
        try {
          return std::make_shared<iceberg::ice_tea::RESTCatalog>(host, port);
        } catch (...) {
          TEA_LOG("Failed to connect to REST catalog (" + host + ":" + std::to_string(port) + ")");
        }
      }
#endif
      throw std::runtime_error("No correct REST endpoints for iceberg catalog were provided");
    }
  }
  throw std::runtime_error("No any correct endpoint for iceberg catalog were provided");
}

class FilteringEntriesStream : public iceberg::ice_tea::IcebergEntriesStream {
 public:
  FilteringEntriesStream(std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> input, iceberg::filter::NodePtr expr,
                         std::shared_ptr<iceberg::Schema> schema, int64_t timestamp_to_timestamptz_shift_us)
      : input_(input),
        filter_(expr, iceberg::filter::StatsFilter::Settings{.timestamp_to_timestamptz_shift_us =
                                                                 timestamp_to_timestamptz_shift_us}),
        schema_(schema) {}

  std::optional<iceberg::ManifestEntry> ReadNext() override {
    while (true) {
      auto maybe_entry = input_->ReadNext();
      if (!maybe_entry.has_value()) {
        return std::nullopt;
      }

      iceberg::ManifestEntryStatsGetter stats_getter(maybe_entry.value(), schema_);
      if (filter_.ApplyFilter(stats_getter) == iceberg::filter::MatchedRows::kNone) {
        continue;
      }

      return maybe_entry.value();
    }
  }

 private:
  std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> input_;
  iceberg::filter::StatsFilter filter_;
  std::shared_ptr<iceberg::Schema> schema_;
};

std::string GetIcebergTableLocation(const Config& config, TableId table_id) {
  auto catalog = GetCatalog(config);

  iceberg::catalog::TableIdentifier table_ident{.db = table_id.db_name, .name = table_id.table_name};
  auto table = catalog->LoadTable(table_ident);
  if (!table) {
    throw arrow::Status::ExecutionError("Cannot open Iceberg table ", table_id.db_name, ".", table_id.table_name);
  }
  return table->Location();
}

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromIcebergWithLocation(
    iceberg::filter::NodePtr filter, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    const std::string& location, int64_t timestamp_to_timestamptz_shift_us,
    std::function<bool(iceberg::Schema& schema)> use_avro_reader_schema,
    iceberg::filter::NodePtr partition_pruning_filter) {
  PlannerStats stats;
  std::optional<ScopedTimerTicks> timer = ScopedTimerTicks(stats.plan_duration);

  TEA_LOG("Snapshot file " + location);
  auto maybe_fs = fs_provider->GetFileSystem(location);
  if (!maybe_fs.ok()) {
    throw maybe_fs.status();
  }
  std::shared_ptr<arrow::fs::FileSystem> fs = maybe_fs.ValueUnsafe();
  auto metrics = std::make_shared<IcebergMetrics>(IcebergMetrics{});
  fs = std::make_shared<IcebergLoggingFileSystem>(fs, metrics);

  auto result = [&]() -> arrow::Result<iceberg::ice_tea::ScanMetadata> {
    ARROW_ASSIGN_OR_RAISE(auto data, iceberg::ice_tea::ReadFile(fs, location));
    std::shared_ptr<iceberg::TableMetadataV2> table_metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
    if (!table_metadata) {
      return arrow::Status::ExecutionError("GetScanMetadata: failed to parse metadata " + location);
    }
    auto schema = table_metadata->GetCurrentSchema();
    if (!schema) {
      return arrow::Status::ExecutionError("GetScanMetadata: failed to parse metadata " + location +
                                           " (schema not found)");
    }

    std::shared_ptr<iceberg::filter::StatsFilter> partition_pruning_stats_filter;
    if (partition_pruning_filter) {
      partition_pruning_stats_filter = std::make_shared<iceberg::filter::StatsFilter>(
          partition_pruning_filter, iceberg::filter::StatsFilter::Settings{});
    }

    bool use_reader_schema = filter ? use_avro_reader_schema(*schema) : false;
    std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> entries_stream = iceberg::ice_tea::AllEntriesStream::Make(
        fs, table_metadata, use_reader_schema, partition_pruning_stats_filter,
        filter ? MakeScanDeserializerConfigWithFilter() : MakeFullScanDeserializerConfig());
    if (filter) {
      entries_stream = std::make_shared<FilteringEntriesStream>(
          entries_stream, filter, table_metadata->GetCurrentSchema(), timestamp_to_timestamptz_shift_us);
    }

    return iceberg::ice_tea::GetScanMetadata(*entries_stream, *table_metadata);
  }();

  if (result.ok()) {
    // stats.plan_duration is correct only after timer is destroyed
    timer.reset();

    stats.iceberg_fs_duration = metrics->filesystem_duration;
    stats.iceberg_bytes_read = metrics->bytes_read;
    stats.iceberg_files_read = metrics->files_opened;
    stats.iceberg_requests = metrics->requests;
    return std::make_pair(result.MoveValueUnsafe(), std::move(stats));
  } else {
    throw result.status();
  }
}

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromIceberg(
    const Config& config, TableId table_id, iceberg::filter::NodePtr filter,
    std::shared_ptr<iceberg::IFileSystemProvider> fs_provider, int64_t timestamp_to_timestamptz_shift_us,
    iceberg::filter::NodePtr partition_pruning_filter) {
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
      partition_pruning_filter);
  stats.Combine(fs_stats);
  return {meta, stats};
}

}  // namespace tea::meta::access
