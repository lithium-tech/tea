#include "tea/common/batch_size.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <numeric>
#include <string>

#include "iceberg/common/error.h"

namespace tea {

uint64_t CalculateBatchSize(const std::vector<uint64_t>& column_sizes, uint64_t rows,
                            uint64_t max_bytes_for_column_in_batch, uint64_t max_bytes_for_batch) {
  uint64_t total_uncompressed_size =
      std::accumulate(column_sizes.begin(), column_sizes.end(), static_cast<uint64_t>(0));

  double bytes_per_row = static_cast<double>(total_uncompressed_size) / (rows + 1) + 1;
  uint64_t batch_size_to_use = max_bytes_for_batch / bytes_per_row;

  for (uint64_t column_size : column_sizes) {
    double bytes_per_value = static_cast<double>(column_size) / (rows + 1) + 1;
    uint64_t max_allowed_batch_size = max_bytes_for_column_in_batch / bytes_per_value;

    batch_size_to_use = std::min(batch_size_to_use, max_allowed_batch_size);
  }

  batch_size_to_use = std::min(kBatchRowsUpperBound, batch_size_to_use);
  batch_size_to_use = std::max(kBatchRowsLowerBound, batch_size_to_use);

  return batch_size_to_use;
}

std::map<int32_t, uint64_t> UncompressedColumnSizes(std::shared_ptr<const parquet::RowGroupMetaData> metadata) {
  const parquet::SchemaDescriptor* schema = metadata->schema();
  iceberg::Ensure(schema != nullptr, std::string(__PRETTY_FUNCTION__) + ": schema is nullptr");

  std::map<int32_t, std::vector<int>> field_id_to_columns;
  int num_columns = schema->num_columns();
  for (int col_idx = 0; col_idx < num_columns; ++col_idx) {
    const parquet::schema::Node* node = schema->GetColumnRoot(col_idx);
    iceberg::Ensure(node != nullptr, std::string(__PRETTY_FUNCTION__) + ": schema->GetColumnRoot(" +
                                         std::to_string(col_idx) + ") is nullptr");

    int32_t field_id = node->field_id();
    if (field_id != -1) {
      field_id_to_columns[field_id].push_back(col_idx);
    }
  }

  std::map<int32_t, uint64_t> column_sizes;
  for (const auto& [field_id, col_indices] : field_id_to_columns) {
    for (int col_idx : col_indices) {
      std::unique_ptr<parquet::ColumnChunkMetaData> column_chunk = metadata->ColumnChunk(col_idx);
      iceberg::Ensure(column_chunk != nullptr, std::string(__PRETTY_FUNCTION__) + ": metadata->ColumnChunk(" +
                                                   std::to_string(col_idx) + ") is nullptr");

      int64_t uncompressed_size = column_chunk->total_uncompressed_size();
      column_sizes[field_id] += uncompressed_size;
    }
  }

  return column_sizes;
}

}  // namespace tea
