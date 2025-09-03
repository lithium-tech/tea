#pragma once

#include <arrow/util/compression.h>
#include <arrow/util/type_fwd.h>

#include <memory>
#include <string>

#include "tea/compression/compressor.h"

namespace tea::compression {

class ArrowBasedCompressor : public ICompressor {
 public:
  explicit ArrowBasedCompressor(arrow::Compression::type type);

  void Compress(std::string& data) override;
  void Decompress(std::string& data) override;

 private:
  std::shared_ptr<arrow::util::Codec> codec_;
};

}  // namespace tea::compression
