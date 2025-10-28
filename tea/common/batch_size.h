#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "parquet/metadata.h"

namespace tea {

constexpr uint64_t kBatchRowsLowerBound = 128;
constexpr uint64_t kBatchRowsUpperBound = 131072;

uint64_t CalculateBatchSize(const std::vector<uint64_t>& column_sizes, uint64_t rows,
                            uint64_t max_bytes_for_column_in_batch, uint64_t max_bytes_for_batch);

std::map<int32_t, uint64_t> UncompressedColumnSizes(std::shared_ptr<const parquet::RowGroupMetaData> metadata);

}  // namespace tea
