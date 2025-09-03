#pragma once

#include <string>
#include <unordered_map>

#include "tea/compression/compressor.h"

namespace tea::compression {

class CompressorFactory {
 public:
  CompressorFactory();

  CompressorPtr GetCompressor(const std::string& compressor_name) const;

  void RegisterCompressor(CompressorPtr provider);

 private:
  std::unordered_map<std::string, CompressorPtr> compressors_;
};

}  // namespace tea::compression
