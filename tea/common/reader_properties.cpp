#include "tea/common/reader_properties.h"

namespace tea {

parquet::ReaderProperties ReaderProperties::GetParquetReaderProperties() const {
  auto properties = parquet::default_reader_properties();
  if (force_buffered_) {
    properties.enable_buffered_stream();
  } else {
    properties.disable_buffered_stream();
  }

  if (config_ && config_->limits.parquet_buffer_size) {
    properties.enable_buffered_stream();
    properties.set_buffer_size(config_->limits.parquet_buffer_size);
  }
  return properties;
}

}  // namespace tea
