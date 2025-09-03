#include "tea/compression/arrow_codec.h"

#include <utility>

namespace tea::compression {

ArrowBasedCompressor::ArrowBasedCompressor(arrow::Compression::type type) {
  auto maybe_codec = arrow::util::Codec::Create(type);
  if (!maybe_codec.ok()) {
    throw std::runtime_error("Error in creating codec with id " + std::to_string(type));
  }
  codec_ = std::move(maybe_codec.ValueUnsafe());
}

void ArrowBasedCompressor::Compress(std::string& data) {
  int64_t max_compressed_size = codec_->MaxCompressedLen(data.size(), nullptr);

  std::string compressed;
  compressed.resize(sizeof(int64_t) + max_compressed_size);

  int64_t original_size = static_cast<int64_t>(data.size());
  std::memcpy(&compressed[0], &original_size, sizeof(int64_t));

  auto maybe_compressed_size =
      codec_->Compress(data.size(), reinterpret_cast<const uint8_t*>(data.data()), max_compressed_size,
                       reinterpret_cast<uint8_t*>(&compressed[sizeof(int64_t)]));
  if (!maybe_compressed_size.ok()) {
    throw std::runtime_error("Error in compressing data with LZ4");
  }

  int64_t compressed_size = maybe_compressed_size.ValueUnsafe();
  compressed.resize(sizeof(int64_t) + compressed_size);

  data = compressed;
}

void ArrowBasedCompressor::Decompress(std::string& data) {
  if (data.size() < sizeof(int64_t)) {
    throw std::runtime_error("Compressed data too small");
  }

  int64_t original_size;
  std::memcpy(&original_size, data.data(), sizeof(int64_t));

  std::string decompressed;
  decompressed.resize(original_size);

  auto maybe_decompressed_size =
      codec_->Decompress(data.size() - sizeof(int64_t), reinterpret_cast<const uint8_t*>(&data[sizeof(int64_t)]),
                         original_size, reinterpret_cast<uint8_t*>(&decompressed[0]));
  if (!maybe_decompressed_size.ok()) {
    throw std::runtime_error("Error in decompressing data");
  }

  int64_t decompressed_size = maybe_decompressed_size.ValueUnsafe();
  decompressed.resize(decompressed_size);
  data = decompressed;
}

}  // namespace tea::compression
