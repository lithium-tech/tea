#pragma once

#include <string>

#include "tea/compression/compressor.h"

namespace tea::compression {

static constexpr std::string_view kIdentityCompressorName = "identity";

class IdentityCompressor : public ICompressor {
 public:
  IdentityCompressor() = default;

  std::string GetName() const override;
  void Compress(std::string& data) override;
  void Decompress(std::string& data) override;
};

}  // namespace tea::compression
