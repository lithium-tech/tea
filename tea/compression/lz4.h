#pragma once

#include <string>

#include "tea/compression/arrow_codec.h"

namespace tea::compression {

static constexpr std::string_view kLz4CompressorName = "lz4";

class LZ4Compressor : public ArrowBasedCompressor {
 public:
  LZ4Compressor();

  std::string GetName() const override;
};

}  // namespace tea::compression
