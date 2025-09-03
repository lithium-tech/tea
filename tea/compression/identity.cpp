#include "tea/compression/identity.h"

namespace tea::compression {

std::string IdentityCompressor::GetName() const { return std::string(kIdentityCompressorName); }

void IdentityCompressor::Compress(std::string& data) {}

void IdentityCompressor::Decompress(std::string& data) {}

}  // namespace tea::compression
