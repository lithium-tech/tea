#include "tea/common/file_reader_provider.h"

#include <algorithm>
#include <map>
#include <string>

#include "arrow/filesystem/filesystem.h"
#include "arrow/status.h"
#include "iceberg/common/fs/url.h"
#include "parquet/metadata.h"

#include "tea/common/batch_size.h"

namespace tea {
namespace {
arrow::Result<std::string> GetPath(const std::string& url) {
  using std::literals::string_view_literals::operator""sv;

  auto components = iceberg::SplitUrl(url);
  std::string path;

  std::shared_ptr<arrow::fs::FileSystem> fs;
  if (components.schema == "s3a"sv || components.schema == "s3"sv) {
    path = (static_cast<std::string>(components.location) + static_cast<std::string>(components.path));
  } else if (components.schema == "file"sv) {
    path = components.path;
  } else {
    return ::arrow::Status::ExecutionError("unknown fs prefix for file: ", url);
  }
  return path;
}

uint64_t CalculateBatchSizeRowGroup(const FileReaderProviderWithProperties::AdaptiveBatchInfo& info,
                                    std::shared_ptr<parquet::RowGroupMetaData> row_group_metadata,
                                    const std::set<int32_t>& field_ids_to_retrieve) {
  const std::map<int32_t, uint64_t> field_id_to_uncompressed_column_size = UncompressedColumnSizes(row_group_metadata);

  std::vector<uint64_t> column_sizes;
  column_sizes.reserve(field_ids_to_retrieve.size());

  for (int32_t id : field_ids_to_retrieve) {
    auto it = field_id_to_uncompressed_column_size.find(id);
    if (it == field_id_to_uncompressed_column_size.end()) {
      column_sizes.emplace_back(0);
    } else {
      column_sizes.emplace_back(it->second);
    }
  }

  return CalculateBatchSize(column_sizes, row_group_metadata->num_rows(), info.max_bytes_in_column,
                            info.max_bytes_in_batch);
}

uint64_t CalculateBatchSizeFile(const FileReaderProviderWithProperties::AdaptiveBatchInfo& info,
                                std::shared_ptr<parquet::FileMetaData> file_metadata,
                                const std::set<int32_t>& field_ids_to_retrieve) {
  const int32_t num_row_groups = file_metadata->num_row_groups();
  uint64_t batch_size = info.max_rows;

  for (int32_t i = 0; i < num_row_groups; ++i) {
    uint64_t batch_size_for_rg = CalculateBatchSizeRowGroup(info, file_metadata->RowGroup(i), field_ids_to_retrieve);
    batch_size = std::min(batch_size, batch_size_for_rg);
  }

  batch_size = std::max(batch_size, info.min_rows);
  return batch_size;
}

}  // namespace

arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> FileReaderProviderWithProperties::Open(
    const std::string& url) const {
  ARROW_ASSIGN_OR_RAISE(auto fs, fs_provider_->GetFileSystem(url));
  ARROW_ASSIGN_OR_RAISE(auto path, GetPath(url));
  ARROW_ASSIGN_OR_RAISE(auto input_file, fs->OpenInputFile(path));

  auto props = properties_.GetParquetReaderProperties();
  auto arrow_props = properties_.GetArrowReaderProperties();

  std::shared_ptr<parquet::FileMetaData> parquet_metadata = parquet::ReadMetaData(input_file);

  if (adaptive_batch_info_.has_value()) {
    const auto& info = adaptive_batch_info_.value();

    uint64_t batch_size = CalculateBatchSizeFile(info, parquet_metadata, field_ids_to_retrieve_);
    arrow_props.set_batch_size(batch_size);
  }

  parquet::arrow::FileReaderBuilder reader_builder;
  ARROW_RETURN_NOT_OK(reader_builder.Open(input_file, props, parquet_metadata));
  reader_builder.memory_pool(arrow::default_memory_pool());
  reader_builder.properties(arrow_props);

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());

  return arrow_reader;
}

arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> CachingFileReaderProvider::Open(
    const std::string& url) const {
  try {
    return metadata_cache_.GetValueOrCalculate(url, [provider = provider_, &url]() {
      auto maybe_arrow_reader = provider->Open(url);
      if (!maybe_arrow_reader.ok()) {
        throw maybe_arrow_reader.status();
      }
      std::shared_ptr<parquet::arrow::FileReader> result = maybe_arrow_reader.MoveValueUnsafe();
      return result;
    });
  } catch (const arrow::Status& status) {
    return status;
  }
}

}  // namespace tea
