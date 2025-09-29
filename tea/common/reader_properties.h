#pragma once

#include <optional>

#include "parquet/arrow/reader.h"

#include "tea/common/config.h"

namespace tea {

class ReaderProperties {
 public:
  ReaderProperties() = default;
  explicit ReaderProperties(const Config& config) : config_(config) {}

  parquet::ReaderProperties GetParquetReaderProperties() const;

  parquet::ArrowReaderProperties GetArrowReaderProperties() const {
    auto result = parquet::default_arrow_reader_properties();
    if (config_.has_value()) {
      result.set_batch_size(config_->limits.arrow_buffer_rows);
    }
    if (config_.has_value()) {
      result.set_use_threads(config_->features.read_in_multiple_threads);
    }
    return result;
  }

  void ForceBuffered(bool value) { force_buffered_ = value; }

 private:
  std::optional<Config> config_;
  bool force_buffered_ = false;
};

}  // namespace tea
