#pragma once

#include <memory>
#include <string>
#include <utility>

#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "parquet/arrow/reader.h"

#include "tea/common/reader_properties.h"
#include "tea/util/lru_cache.h"

namespace tea {

class FileReaderProviderWithProperties : public iceberg::IFileReaderProvider {
 public:
  FileReaderProviderWithProperties(ReaderProperties properties,
                                   std::shared_ptr<iceberg::IFileSystemProvider> fs_provider)
      : properties_(std::move(properties)), fs_provider_(fs_provider) {}

  arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> Open(const std::string& url) const override;

 private:
  ReaderProperties properties_;
  std::shared_ptr<iceberg::IFileSystemProvider> fs_provider_;
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
