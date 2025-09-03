#include "tea/compression/lz4.h"

#include <arrow/util/type_fwd.h>

#include "tea/compression/arrow_codec.h"

namespace tea::compression {

std::string LZ4Compressor::GetName() const { return std::string(kLz4CompressorName); }

LZ4Compressor::LZ4Compressor() : ArrowBasedCompressor(arrow::Compression::LZ4) {}

}  // namespace tea::compression
