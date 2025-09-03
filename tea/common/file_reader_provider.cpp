#include "tea/common/file_reader_provider.h"

#include <string>

#include "arrow/filesystem/filesystem.h"
#include "arrow/status.h"
#include "iceberg/common/fs/url.h"

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
}  // namespace

arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> FileReaderProviderWithProperties::Open(
    const std::string& url) const {
  auto props = properties_.GetParquetReaderProperties();
  auto arrow_props = properties_.GetArrowReaderProperties();
  ARROW_ASSIGN_OR_RAISE(auto fs, fs_provider_->GetFileSystem(url));
  ARROW_ASSIGN_OR_RAISE(auto path, GetPath(url));
  ARROW_ASSIGN_OR_RAISE(auto input_file, fs->OpenInputFile(path));

  parquet::arrow::FileReaderBuilder reader_builder;
  ARROW_RETURN_NOT_OK(reader_builder.Open(input_file, props));
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
