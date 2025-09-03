#include "tea/compression/snappy.h"

#include <arrow/util/type_fwd.h>

#include "tea/compression/arrow_codec.h"

namespace tea::compression {

std::string SnappyCompressor::GetName() const { return std::string(kSnappyCompressorName); }

SnappyCompressor::SnappyCompressor() : ArrowBasedCompressor(arrow::Compression::SNAPPY) {}

}  // namespace tea::compression
