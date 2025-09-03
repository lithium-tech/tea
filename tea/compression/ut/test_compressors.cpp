#include <random>

#include "gtest/gtest.h"

#include "tea/compression/compression_registry.h"
#include "tea/compression/identity.h"
#include "tea/compression/lz4.h"
#include "tea/compression/snappy.h"

namespace tea::compression {

void TestCompressor(const std::string_view& compressor_name, int num_tests) {
  auto compressor = CompressorFactory().GetCompressor(std::string(compressor_name));

  unsigned int seed = 42;

  std::mt19937 generator(seed);
  std::uniform_int_distribution<int> distribution(0, 255);

  for (int i = 0; i < num_tests; ++i) {
    int length_str = distribution(generator) % 100 + 1;
    std::string data;
    data.reserve(length_str);

    for (int j = 0; j < length_str; ++j) {
      data.push_back(distribution(generator) - 128);
    }

    auto initial_data = data;
    compressor->Compress(data);
    compressor->Decompress(data);
    EXPECT_EQ(data, initial_data);
  }
}

TEST(Compression, Snappy) { TestCompressor(kSnappyCompressorName, 10); }
TEST(Compression, Identity) { TestCompressor(kIdentityCompressorName, 10); }
TEST(Compression, Unset) { TestCompressor("", 10); }
TEST(Compression, LZ4) { TestCompressor(kLz4CompressorName, 10); }

}  // namespace tea::compression
