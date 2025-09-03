#include "tea/common/row_groups_utils.h"

#include <string>

#include "iceberg/common/error.h"
#include "iceberg/common/rg_metadata.h"
#include "iceberg/tea_scan.h"

namespace tea {

// TODO(gmusya): reuse this code in samovar
std::vector<RowGroupInfo> PrepareRowGroups(std::shared_ptr<const parquet::FileMetaData> metadata) {
  std::vector<RowGroupInfo> row_groups;

  auto num_row_groups = metadata->num_row_groups();
  row_groups.clear();
  row_groups.reserve(num_row_groups);
  int64_t num_rows_before = 0;
  for (int i = 0; i < num_row_groups; ++i) {
    auto row_group = metadata->RowGroup(i);
    iceberg::Ensure(row_group.get(), std::string(__PRETTY_FUNCTION__) + ": no RowGroup '" + std::to_string(i) + "'");
    auto row_group_offset = iceberg::RowGroupMetaToFileOffset(*row_group);

    row_groups.push_back(RowGroupInfo{
        .offset = row_group_offset, .start_row_number = num_rows_before, .rows_count = row_group->num_rows()});
    num_rows_before += row_group->num_rows();
  }
  return row_groups;
}

std::vector<bool> GetNeedToProcessRowGroups(const iceberg::ice_tea::DataEntry& data_entry,
                                            const std::vector<RowGroupInfo>& row_group_info) {
  std::vector<bool> need_to_process_row_group;

  const auto& parts = data_entry.parts;
  need_to_process_row_group.assign(row_group_info.size(), false);
  size_t last_not_mapped_part_id = 0;
  for (size_t i = 0; i < row_group_info.size(); ++i) {
    int64_t file_offset = row_group_info[i].offset;
    while (last_not_mapped_part_id < parts.size()) {
      const auto seg_length = parts[last_not_mapped_part_id].length;  // 0 <=> to end
      const auto& seg_start = parts[last_not_mapped_part_id].offset;
      const auto& seg_end = parts[last_not_mapped_part_id].offset + parts[last_not_mapped_part_id].length;

      if (seg_start <= file_offset && (file_offset < seg_end || seg_length == 0)) {
        need_to_process_row_group[i] = true;
        break;
      }
      if (file_offset >= seg_end) {
        ++last_not_mapped_part_id;
        continue;
      }
      break;
    }
  }
  return need_to_process_row_group;
}

}  // namespace tea
