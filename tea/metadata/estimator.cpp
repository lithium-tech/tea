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
#include "tea/metadata/access_iceberg.h"
#include "tea/metadata/entries_stream_config.h"

namespace tea::meta {
namespace {

struct IcebergInfo {
  std::shared_ptr<iceberg::TableMetadataV2> table_metadata;
  std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> entries_stream;
};

arrow::Result<IcebergInfo> IcebergInfoFromConfig(const Config& config, TableId table_id,
                                                 std::shared_ptr<iceberg::IFileSystemProvider> fs_provider) {
  std::string metadata_location = access::GetIcebergTableLocation(config, table_id);

  ARROW_ASSIGN_OR_RAISE(auto fs, fs_provider->GetFileSystem(metadata_location));

  ARROW_ASSIGN_OR_RAISE(auto data, iceberg::ice_tea::ReadFile(fs, metadata_location));
  std::shared_ptr<iceberg::TableMetadataV2> table_metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  if (!table_metadata) {
    return arrow::Status::ExecutionError("GetScanMetadata: failed to parse metadata " + metadata_location);
  }

  std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> entries_stream =
      iceberg::ice_tea::AllEntriesStream::Make(fs, table_metadata, false, nullptr, MakeFullScanDeserializerConfig());

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

#ifdef TEA_BUILD_STATS
std::optional<int64_t> GetValueCounts(const iceberg::ManifestEntry& entry, int32_t field_id) {
  for (const auto& [id, cnt] : entry.data_file.value_counts) {
    if (id == field_id) {
      return cnt;
    }
  }
  return std::nullopt;
}

std::optional<int64_t> GetColumnSize(const iceberg::ManifestEntry& entry, int32_t field_id) {
  for (const auto& [id, cnt] : entry.data_file.column_sizes) {
    if (id == field_id) {
      return cnt;
    }
  }
  return std::nullopt;
}

ColumnStats GetColumnStats(const iceberg::ManifestEntry& entry, int32_t field_id) {
  uint64_t total_rows = entry.data_file.record_count;
  std::optional<int64_t> value_count = GetValueCounts(entry, field_id);
  std::optional<int64_t> column_size = GetColumnSize(entry, field_id);

  ColumnStats result{.null_count = -1,
                     .distinct_count = -1,
                     .not_null_count = -1,
                     .total_compressed_size = -1,
                     .total_uncompressed_size = -1};

  if (value_count.has_value()) {
    result.not_null_count = value_count.value();
    result.null_count = total_rows - value_count.value();
  }

  if (column_size.has_value()) {
    result.total_compressed_size = column_size.value();
    // TODO(gmusya): estimate total_uncompressed_size
  }

  return result;
}

void Add(ColumnStats& base, const ColumnStats& addition) {
  base.distinct_count = -1;

  if (addition.not_null_count == -1 || base.not_null_count == -1) {
    base.not_null_count = -1;
  } else {
    base.not_null_count += addition.not_null_count;
  }

  if (addition.null_count == -1 || base.null_count == -1) {
    base.null_count = -1;
  } else {
    base.null_count += addition.null_count;
  }

  if (addition.total_uncompressed_size == -1 || base.total_uncompressed_size == -1) {
    base.total_uncompressed_size = -1;
  } else {
    base.total_uncompressed_size += addition.total_uncompressed_size;
  }

  if (addition.total_compressed_size == -1 || base.total_compressed_size == -1) {
    base.total_compressed_size = -1;
  } else {
    base.total_compressed_size += addition.total_compressed_size;
  }
}
#endif

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

std::shared_ptr<iceberg::Snapshot> GetCurrentSnapshot(std::shared_ptr<iceberg::TableMetadataV2> metadata) {
  iceberg::Ensure(metadata->current_snapshot_id.has_value(),
                  std::string(__PRETTY_FUNCTION__) + ": failed to get current snapshot_id");
  int64_t current_snapshot_id = metadata->current_snapshot_id.value();

  for (const auto& snapshot : metadata->snapshots) {
    if (snapshot->snapshot_id == current_snapshot_id) {
      return snapshot;
    }
  }

  throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": failed to get current snapshot");
}

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
    const Config& config, TableId table_id, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider) {
  std::string metadata_location = access::GetIcebergTableLocation(config, table_id);

  std::shared_ptr<arrow::fs::FileSystem> fs = iceberg::ValueSafe(fs_provider->GetFileSystem(metadata_location));
  std::string data = iceberg::ValueSafe(iceberg::ice_tea::ReadFile(fs, metadata_location));
  std::shared_ptr<iceberg::TableMetadataV2> table_metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  if (!table_metadata) {
    throw std::runtime_error("GetReltuplesFromIceberg: failed to parse metadata " + metadata_location);
  }

  std::shared_ptr<iceberg::Snapshot> snapshot = GetCurrentSnapshot(table_metadata);
  return GetTotalMetricFromSnapshot(snapshot);
}

arrow::Result<RelationSize> Estimator::GetRelationSizeFromIceberg(
    const Config& config, TableId table_id, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider) {
  ARROW_ASSIGN_OR_RAISE(auto iceberg_info, IcebergInfoFromConfig(config, table_id, fs_provider));
  auto table_metadata = iceberg_info.table_metadata;
  auto entries_stream = iceberg_info.entries_stream;

  auto schema = table_metadata->GetCurrentSchema();
  TableStatsAggregator agg(schema->Columns());

  ForEachDataEntry(entries_stream, [&](const iceberg::ManifestEntry& entry) { agg.AddManifestEntry(entry); });

  return RelationSize{.rows = double(agg.GetRows()), .width = static_cast<int>(agg.GetWidth())};
}

arrow::Result<RelationSize> Estimator::GetRelationSizeFromDataFiles(
    const iceberg::ice_tea::ScanMetadata& metadata, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    ReaderProperties props) {
  int64_t rows = 0;
  props.ForceBuffered(true);
  auto file_reader_provider = std::make_shared<FileReaderProviderWithProperties>(std::move(props), fs_provider);

  ForEachPlannedDataEntry(metadata, [&rows, file_reader_provider](const iceberg::ice_tea::DataEntry& entry) {
    auto maybe_arrow_reader = file_reader_provider->Open(entry.path);
    if (!maybe_arrow_reader.ok()) {
      throw maybe_arrow_reader.status();
    }
    auto arrow_reader = maybe_arrow_reader.MoveValueUnsafe();

    auto parquet_metadata = arrow_reader->parquet_reader()->metadata();
    rows += parquet_metadata->num_rows();
  });
  return RelationSize{.rows = double(rows), .width = 40};
}

#ifdef TEA_BUILD_STATS
arrow::Result<ColumnStats> Estimator::GetIcebergColumnStats(const Config& config, TableId table_id,
                                                            const std::string& column_name) {
  ARROW_ASSIGN_OR_RAISE(auto iceberg_info, IcebergInfoFromConfig(config, table_id));
  auto table_metadata = iceberg_info.table_metadata;
  auto entries_stream = iceberg_info.entries_stream;

  int field_id = -1;
  {
    auto maybe_field_id = table_metadata->GetCurrentSchema()->FindColumnIgnoreCase(column_name);
    if (!maybe_field_id.has_value()) {
      return arrow::Status::ExecutionError("GetIcebergColumnStats: Column ", column_name, " not found in schema");
    }
    field_id = maybe_field_id.value();
  }

  ColumnStats result{.null_count = 0,
                     .distinct_count = 0,
                     .not_null_count = 0,
                     .total_compressed_size = 0,
                     .total_uncompressed_size = 0};

  ForEachDataEntry(entries_stream, [&](const iceberg::ManifestEntry& entry) {
    ColumnStats task_stats = GetColumnStats(entry, field_id);
    Add(result, task_stats);
  });

  return result;
}
#endif

}  // namespace tea::meta
