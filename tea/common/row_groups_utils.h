#pragma once

#include <memory>
#include <vector>

#include "iceberg/tea_scan.h"
#include "parquet/metadata.h"

namespace tea {

struct RowGroupInfo {
  int64_t offset = 0;
  int64_t start_row_number = 0;
  int64_t rows_count = 0;
};

std::vector<RowGroupInfo> PrepareRowGroups(std::shared_ptr<const parquet::FileMetaData> metadata);

std::vector<bool> GetNeedToProcessRowGroups(const iceberg::ice_tea::DataEntry& data_entry,
                                            const std::vector<RowGroupInfo>& row_group_info);

}  // namespace tea
