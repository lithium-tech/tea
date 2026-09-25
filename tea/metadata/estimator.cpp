#include "tea/metadata/estimator.h"

#include <arrow/filesystem/filesystem.h>
#include <iceberg/manifest_entry.h>
#include <iceberg/result.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>
#include <iceberg/tea_scan.h>

#include <algorithm>
#include <map>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "absl/strings/ascii.h"
#include "arrow/status.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/tea_column_stats.h"

#include "tea/common/file_reader_provider.h"
#include "tea/common/reader_properties.h"
#include "tea/common/utils.h"
#include "tea/metadata/access_iceberg.h"
#include "tea/metadata/entries_stream_config.h"

namespace tea::meta {
namespace {

struct IcebergInfo {
  std::shared_ptr<iceberg::TableMetadataV2> table_metadata;
  std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> entries_stream;
};

std::shared_ptr<iceberg::Snapshot> FindSnapshot(std::shared_ptr<iceberg::TableMetadataV2> table_metadata,
                                                const SnapshotRef& snapshot_ref) {
  auto snapshot_id = ResolveSnapshotId(table_metadata, snapshot_ref);
  if (!snapshot_id.has_value()) {
    return nullptr;
  }

  for (const auto& snapshot : table_metadata->snapshots) {
    if (snapshot->snapshot_id == *snapshot_id) {
      return snapshot;
    }
  }
  throw std::runtime_error("Snapshot with ID " + std::to_string(*snapshot_id) + " not found in table metadata");
}

arrow::Result<IcebergInfo> IcebergInfoFromConfig(const Config& config, TableId table_id,
                                                 std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
                                                 const SnapshotRef& snapshot_ref = CurrentSnapshot{}) {
  std::string metadata_location = access::GetIcebergTableLocation(config, table_id);

  ARROW_ASSIGN_OR_RAISE(auto fs, fs_provider->GetFileSystem(metadata_location));

  ARROW_ASSIGN_OR_RAISE(auto data, iceberg::ice_tea::ReadFile(fs, metadata_location));
  std::shared_ptr<iceberg::TableMetadataV2> table_metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  if (!table_metadata) {
    return arrow::Status::ExecutionError("GetScanMetadata: failed to parse metadata " + metadata_location);
  }

  auto schema = tea::GetSchemaForSnapshot(table_metadata, snapshot_ref);

  auto manifest_list_path = tea::GetManifestListPathForSnapshot(table_metadata, snapshot_ref);
  if (!manifest_list_path.has_value()) {
    // TODO(gmusya): handle empty Iceberg tables in estimator.
    return arrow::Status::ExecutionError("Manifest list path not found for snapshot");
  }

  std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> entries_stream =
      iceberg::ice_tea::AllEntriesStream::Make(fs, manifest_list_path.value(), false, table_metadata->partition_specs,
                                               schema, nullptr, MakeFullScanDeserializerConfig());

  return IcebergInfo{.table_metadata = table_metadata, .entries_stream = entries_stream};
}

static void ForEachPlannedDataEntry(const iceberg::ice_tea::ScanMetadata& metadata,
                                    const std::function<void(const iceberg::ice_tea::DataEntry&)>& f) {
  for (const auto& partition : metadata.partitions) {
    for (const auto& layer : partition) {
      for (const auto& data_entry : layer.data_entries_) {
        f(data_entry);
      }
    }
  }
}

void ForEachDataEntry(std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> entries_stream,
                      const std::function<void(const iceberg::ManifestEntry&)>& cb) {
  while (true) {
    auto entry = entries_stream->ReadNext();

    if (!entry.has_value()) {
      return;
    }

    if (entry->data_file.content != iceberg::ContentFile::FileContent::kData) {
      continue;
    }

    cb(*entry);
  }
}

class TableStatsAggregator {
 public:
  explicit TableStatsAggregator(const std::vector<iceberg::types::NestedField>& columns)
      : column_sizes_compressed_(columns.size(), 0), column_value_counts_(columns.size(), 0) {
    for (const auto& ice_field : columns) {
      for (size_t i = 0; i < columns.size(); ++i) {
        field_id_to_column_index_[ice_field.field_id] = i;
      }
    }
  }

  void AddManifestEntry(const iceberg::ManifestEntry& entry) {
    rows_ += entry.data_file.record_count;

    const auto& column_sizes = entry.data_file.column_sizes;
    const auto& value_counts = entry.data_file.value_counts;
    for (const auto& [id, cnt] : column_sizes) {
      if (auto it = field_id_to_column_index_.find(id); it != field_id_to_column_index_.end()) {
        column_sizes_compressed_[it->second] += cnt;
      }
    }
    for (const auto& [id, cnt] : value_counts) {
      if (auto it = field_id_to_column_index_.find(id); it != field_id_to_column_index_.end()) {
        column_value_counts_[it->second] += cnt;
      }
    }
  }

  uint64_t GetRows() const { return rows_; }

  double GetWidth() const {
    double compressed_width = 0;
    for (size_t i = 0; i < column_value_counts_.size(); ++i) {
      if (column_value_counts_[i] != 0) {
        compressed_width += static_cast<double>(column_sizes_compressed_[i]) / column_value_counts_[i];
      }
    }

    // we do not expect large rows, so limit result with value 10000
    constexpr double kMaxExpectedCompressedRowWidth = 10000.0;
    compressed_width = std::min(compressed_width, kMaxExpectedCompressedRowWidth);

    // we cannot estimate decompressed data size properly
    constexpr double kCompressionRatio = 4.2;  // some random number
    double width = compressed_width * kCompressionRatio;

    return width;
  }

 private:
  std::vector<uint64_t> column_sizes_compressed_;
  std::vector<uint64_t> column_value_counts_;

  uint64_t rows_ = 0;

  std::map<int, int> field_id_to_column_index_;
};

std::map<std::string, int64_t> GetTotalMetricFromSnapshot(std::shared_ptr<iceberg::Snapshot> snapshot) {
  std::map<std::string, int64_t> result;
  for (const auto& [key, value] : snapshot->summary) {
    if (key.starts_with("total-")) {
      result[key] = std::stoll(value);
    }
  }
  return result;
}

}  // namespace

std::map<std::string, int64_t> Estimator::GetTotalMetricsFromIceberg(
    const Config& config, TableId table_id, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    SnapshotRef snapshot_ref) {
  std::string metadata_location = access::GetIcebergTableLocation(config, table_id);

  std::shared_ptr<arrow::fs::FileSystem> fs = iceberg::ValueSafe(fs_provider->GetFileSystem(metadata_location));
  std::string data = iceberg::ValueSafe(iceberg::ice_tea::ReadFile(fs, metadata_location));
  std::shared_ptr<iceberg::TableMetadataV2> table_metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  if (!table_metadata) {
    throw std::runtime_error("GetReltuplesFromIceberg: failed to parse metadata " + metadata_location);
  }

  std::shared_ptr<iceberg::Snapshot> snapshot = FindSnapshot(table_metadata, snapshot_ref);
  if (!snapshot) {
    // Table has no snapshots (e.g. a freshly created, empty Iceberg table): report zero metrics
    return {{"total-records", 0},          {"total-data-files", 0},       {"total-files-size", 0},
            {"total-equality-deletes", 0}, {"total-position-deletes", 0}, {"total-delete-files", 0}};
  }

  return GetTotalMetricFromSnapshot(snapshot);
}

arrow::Result<RelationSize> Estimator::GetRelationSizeFromIceberg(
    const Config& config, TableId table_id, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    SnapshotRef snapshot_ref) {
  ARROW_ASSIGN_OR_RAISE(auto iceberg_info, IcebergInfoFromConfig(config, table_id, fs_provider, snapshot_ref));
  auto table_metadata = iceberg_info.table_metadata;
  auto entries_stream = iceberg_info.entries_stream;

  auto schema = tea::GetSchemaForSnapshot(table_metadata, snapshot_ref);
  TableStatsAggregator agg(schema->Columns());

  ForEachDataEntry(entries_stream, [&](const iceberg::ManifestEntry& entry) { agg.AddManifestEntry(entry); });

  return RelationSize{.rows = double(agg.GetRows()), .width = static_cast<int>(agg.GetWidth())};
}

arrow::Result<RelationSize> Estimator::GetRelationSizeFromDataFiles(
    const iceberg::ice_tea::ScanMetadata& metadata, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    ReaderProperties props) {
  int64_t rows = 0;
  props.ForceBuffered(true);
  auto file_reader_provider = std::make_shared<FileReaderProviderWithProperties>(std::move(props), fs_provider,
                                                                                 std::vector<int32_t>{}, std::nullopt);

  ForEachPlannedDataEntry(metadata, [&rows, file_reader_provider](const iceberg::ice_tea::DataEntry& entry) {
    auto maybe_arrow_reader = file_reader_provider->OpenParquet(entry.path);
    if (!maybe_arrow_reader.ok()) {
      throw maybe_arrow_reader.status();
    }
    auto arrow_reader = maybe_arrow_reader.MoveValueUnsafe();

    auto parquet_metadata = arrow_reader->parquet_reader()->metadata();
    rows += parquet_metadata->num_rows();
  });
  return RelationSize{.rows = double(rows), .width = 40};
}

}  // namespace tea::meta
