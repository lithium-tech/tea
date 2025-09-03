#pragma once

#include <string>

#include "tea/compression/arrow_codec.h"

namespace tea::compression {

static constexpr std::string_view kSnappyCompressorName = "snappy";

class SnappyCompressor : public ArrowBasedCompressor {
 public:
  SnappyCompressor();

  std::string GetName() const override;
};

}  // namespace tea::compression
