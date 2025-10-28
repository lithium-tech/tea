#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "parquet/arrow/reader.h"

#include "tea/common/reader_properties.h"
#include "tea/util/lru_cache.h"

namespace tea {

class FileReaderProviderWithProperties : public iceberg::IFileReaderProvider {
 public:
  struct AdaptiveBatchInfo {
    uint64_t max_rows = 1ull << 16;
    uint64_t min_rows = 1ull << 7;
    uint64_t max_bytes_in_batch = 128ull << 20;
    uint64_t max_bytes_in_column = 4ull << 20;
  };

  FileReaderProviderWithProperties(ReaderProperties properties,
                                   std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
                                   const std::vector<int32_t>& field_ids_to_retrieve,
                                   const std::optional<AdaptiveBatchInfo>& adaptive_batch_info = std::nullopt)
      : properties_(std::move(properties)),
        fs_provider_(fs_provider),
        field_ids_to_retrieve_(field_ids_to_retrieve.begin(), field_ids_to_retrieve.end()),
        adaptive_batch_info_(adaptive_batch_info) {}

  arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> Open(const std::string& url) const override;

 private:
  ReaderProperties properties_;
  std::shared_ptr<iceberg::IFileSystemProvider> fs_provider_;
  std::set<int32_t> field_ids_to_retrieve_;
  std::optional<AdaptiveBatchInfo> adaptive_batch_info_;
};

class CachingFileReaderProvider : public iceberg::IFileReaderProvider {
 public:
  CachingFileReaderProvider(std::shared_ptr<const iceberg::IFileReaderProvider> provider, size_t cache_size)
      : provider_(provider), metadata_cache_(cache_size) {}

  arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> Open(const std::string& url) const override;

 private:
  using MetadataCacheValue = std::shared_ptr<parquet::arrow::FileReader>;

  std::shared_ptr<const iceberg::IFileReaderProvider> provider_;
  mutable LRUCache<std::string, MetadataCacheValue> metadata_cache_;
};

}  // namespace tea
