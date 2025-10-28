#include "tea/common/batch_size.h"

#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <random>

#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"

#include "tea/test_utils/common.h"

namespace tea {
namespace {

TEST(AdaptiveBatchSize, BatchBytesLimit) {
  std::vector<uint64_t> column_sizes = {32ull << 20, 32ull << 20, 32ull << 20, 32ull << 20};
  uint64_t rows = 1ull << 20;
  uint64_t max_bytes_for_batch = 8ull << 20;
  uint64_t max_bytes_for_column_in_batch = 8ull << 20;

  uint64_t result = CalculateBatchSize(column_sizes, rows, max_bytes_for_column_in_batch, max_bytes_for_batch);

  uint64_t expected = 64ull << 10;
  EXPECT_NEAR(result, expected, 1000);
}

TEST(AdaptiveBatchSize, ColumnBytesLimit) {
  std::vector<uint64_t> column_sizes = {32ull << 20, 32ull << 20, 32ull << 20, 32ull << 20};
  uint64_t rows = 1ull << 20;
  uint64_t max_bytes_for_batch = 8ull << 20;
  uint64_t max_bytes_for_column_in_batch = 1ull << 20;

  uint64_t result = CalculateBatchSize(column_sizes, rows, max_bytes_for_column_in_batch, max_bytes_for_batch);

  uint64_t expected = 32ull << 10;
  EXPECT_NEAR(result, expected, 1000);
}

TEST(AdaptiveBatchSize, LargeColumnBatchLimit) {
  std::vector<uint64_t> column_sizes = {1ull << 30, 1ull << 20};
  uint64_t rows = 16ull << 10;
  uint64_t max_bytes_for_batch = 128ull << 20;
  uint64_t max_bytes_for_column_in_batch = 8ull << 20;

  uint64_t result = CalculateBatchSize(column_sizes, rows, max_bytes_for_column_in_batch, max_bytes_for_batch);

  uint64_t expected = 1ull << 7;
  EXPECT_NEAR(result, expected, 10);
}

TEST(AdaptiveBatchSize, LargeColumnColumnLimit) {
  std::vector<uint64_t> column_sizes = {1ull << 30, 1ull << 20};
  uint64_t rows = 16ull << 10;
  uint64_t max_bytes_for_batch = 8ull << 20;
  uint64_t max_bytes_for_column_in_batch = 128ull << 20;

  uint64_t result = CalculateBatchSize(column_sizes, rows, max_bytes_for_column_in_batch, max_bytes_for_batch);

  uint64_t expected = 128;
  EXPECT_NEAR(result, expected, 10);
}

TEST(AdaptiveBatchSize, ZeroRows) {
  std::vector<uint64_t> column_sizes = {1ull << 30, 1ull << 20};
  uint64_t rows = 0;
  uint64_t max_bytes_for_batch = 8ull << 20;
  uint64_t max_bytes_for_column_in_batch = 128ull << 20;

  uint64_t result = CalculateBatchSize(column_sizes, rows, max_bytes_for_column_in_batch, max_bytes_for_batch);

  EXPECT_GE(result, kBatchRowsLowerBound);
  EXPECT_LE(result, kBatchRowsUpperBound);
}

TEST(AdaptiveBatchSize, ZeroSizes) {
  std::vector<uint64_t> column_sizes = {0, 0};
  uint64_t rows = 1ull << 30;
  uint64_t max_bytes_for_batch = 128ull << 20;
  uint64_t max_bytes_for_column_in_batch = 128ull << 20;

  uint64_t result = CalculateBatchSize(column_sizes, rows, max_bytes_for_column_in_batch, max_bytes_for_batch);

  EXPECT_GE(result, kBatchRowsLowerBound);
  EXPECT_LE(result, kBatchRowsUpperBound);
}

TEST(AdaptiveBatchSize, NoColumns) {
  std::vector<uint64_t> column_sizes = {};
  uint64_t rows = 1ull << 30;
  uint64_t max_bytes_for_batch = 128ull << 20;
  uint64_t max_bytes_for_column_in_batch = 128ull << 20;

  uint64_t result = CalculateBatchSize(column_sizes, rows, max_bytes_for_column_in_batch, max_bytes_for_batch);

  EXPECT_GE(result, kBatchRowsLowerBound);
  EXPECT_LE(result, kBatchRowsUpperBound);
}

TEST(AdaptiveBatchSize, NoColumnsAndZeroRows) {
  std::vector<uint64_t> column_sizes = {};
  uint64_t rows = 0;
  uint64_t max_bytes_for_batch = 128ull << 20;
  uint64_t max_bytes_for_column_in_batch = 128ull << 20;

  uint64_t result = CalculateBatchSize(column_sizes, rows, max_bytes_for_column_in_batch, max_bytes_for_batch);

  EXPECT_GE(result, kBatchRowsLowerBound);
  EXPECT_LE(result, kBatchRowsUpperBound);
}

TEST(AdaptiveBatchSize, RandomTest) {
  constexpr uint32_t kInvocations = 100;

  std::mt19937 rnd(2101);
  std::uniform_int_distribution<uint32_t> value_log(10, 30);

  auto generate_value = [&]() { return 1ull << value_log(rnd); };

  for (uint32_t invocation = 0; invocation < kInvocations; ++invocation) {
    std::vector<uint64_t> column_sizes = {generate_value(), generate_value(), generate_value(), generate_value()};
    uint64_t rows = generate_value();

    uint64_t max_bytes_for_batch = generate_value();
    uint64_t max_bytes_for_column_in_batch = generate_value();

    uint64_t result_batch_size =
        CalculateBatchSize(column_sizes, rows, max_bytes_for_column_in_batch, max_bytes_for_batch);

    uint64_t total_batch_size_in_bytes = 0;
    uint64_t bytes_per_row = 0;

    for (uint64_t col_size : column_sizes) {
      uint64_t bytes_per_value = col_size / rows;
      uint64_t column_size_in_bytes = bytes_per_value * result_batch_size;
      ASSERT_LE(column_size_in_bytes, max_bytes_for_column_in_batch + bytes_per_value * kBatchRowsLowerBound)
          << "i = " << invocation;

      bytes_per_row += bytes_per_value;
      total_batch_size_in_bytes += column_size_in_bytes;
    }

    ASSERT_LE(total_batch_size_in_bytes, max_bytes_for_batch + bytes_per_row * kBatchRowsLowerBound)
        << "i = " << invocation;
  }
}

TEST(ColumnSize, Float4) {
  auto column1 = MakeJsonColumn("col1", 1, {});
  column1.info.repetition = parquet::Repetition::REPEATED;
  column1.data = ArrayContainer{
      .arrays = {OptionalVector<parquet::ByteArray>{std::nullopt, parquet::ByteArray(R"("qwe")"),
                                                    parquet::ByteArray(R"({"int_value"=1,"string_value"="value"})")},
                 OptionalVector<parquet::ByteArray>{std::nullopt}, OptionalVector<parquet::ByteArray>{std::nullopt}}};

  auto column2 = MakeFloatColumn("col2", 12, OptionalVector<float>{std::nullopt, 2, 3.77});
  auto column3 = MakeFloatColumn("col3", -1, OptionalVector<float>{std::nullopt, 22, 3.77});

  ScopedTempDir dir;
  std::string data_path = dir.path() / "data.parquet";
  ASSERT_OK(WriteToFile({column1, column2, column3}, "file://" + data_path));

  std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(data_path);

  std::map<int32_t, uint64_t> sizes = UncompressedColumnSizes(reader->metadata()->RowGroup(0));
  std::map<int32_t, uint64_t> expected = {{1, 149}, {12, 76}};

  EXPECT_EQ(expected, sizes);
}

}  // namespace
}  // namespace tea
