#include "tea/compression/compression_registry.h"

#include <memory>

#include "tea/compression/identity.h"
#include "tea/compression/lz4.h"
#include "tea/compression/snappy.h"

namespace tea::compression {

CompressorFactory::CompressorFactory() {
  auto lz4_compressor = std::make_shared<LZ4Compressor>();
  auto identity_compressor = std::make_shared<IdentityCompressor>();
  auto snappy_compressor = std::make_shared<SnappyCompressor>();
  compressors_[lz4_compressor->GetName()] = lz4_compressor;
  compressors_[identity_compressor->GetName()] = identity_compressor;
  compressors_[snappy_compressor->GetName()] = snappy_compressor;
  compressors_[""] = identity_compressor;
}

CompressorPtr CompressorFactory::GetCompressor(const std::string& compressor_name) const {
  return compressors_.at(compressor_name);
}

void CompressorFactory::RegisterCompressor(CompressorPtr provider) { compressors_[provider->GetName()] = provider; }

}  // namespace tea::compression
