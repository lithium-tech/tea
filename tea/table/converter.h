#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "iceberg/common/batch.h"

#include "tea/table/bridge.h"
#include "tea/table/gp_fwd.h"

// do not want to include "catalog/pg_type.h"
#define JSONOID 114

namespace tea {

class ArrowToGpConverter {
 public:
  void SetColumnInfo(const std::vector<ReaderColumn>& columns) {
    columns_to_retrieve_.clear();
    for (const auto& column : columns) {
      if (column.remote_only) {
        continue;
      }
      bool substitute_unicode_in_column = false;
      if (encoding_ == Encoding::kCp1251 && (column.gp_type == JSONARRAYOID || column.gp_type == JSONOID) &&
          substitute_unicode_if_json_type_) {
        substitute_unicode_in_column = true;
      }
      columns_to_retrieve_.emplace_back(column.gp_index, column.gp_type, column.gp_name, substitute_unicode_in_column);
    }
  }

  arrow::Status Prepare(iceberg::BatchWithSelectionVector&& batch) {
    cursor_position_ = 0;

    column_id_to_array_.clear();

    for (size_t col_id = 0; col_id < columns_to_retrieve_.size(); ++col_id) {
      const auto& [gp_index, gp_type, parquet_name, substitute_unicode] = columns_to_retrieve_[col_id];
      auto index = batch.GetBatch()->schema()->GetFieldIndex(parquet_name);
      if (index != -1) {
        std::shared_ptr<arrow::Array> column = batch.GetBatch()->column(index);
        column_id_to_array_.emplace_back(col_id, column);
      }
    }

    batch_selection_vector_ = std::move(batch.GetSelectionVector());
    return arrow::Status::OK();
  }

  void SetNonAsciiSubstitutionParameters(bool substitute_unicode_if_json_type) {
    substitute_unicode_if_json_type_ = substitute_unicode_if_json_type;
  }

  bool HasUnreadRows() const { return cursor_position_ < batch_selection_vector_.Size(); }

  arrow::Status SkipRow() {
    if (!HasUnreadRows()) {
      return arrow::Status::ExecutionError(
          "ArrowToGpConverter: internal error in tea. Cursor position is out of bounds");
    }
    ++cursor_position_;
    return arrow::Status::OK();
  }

  arrow::Status ReadRow(GpDatum* values, bool* isnull, CharsetConverter converter) {
    if (!HasUnreadRows()) {
      return arrow::Status::ExecutionError(
          "ArrowToGpConverter: internal error in tea. Cursor position is out of bounds");
    }
    const auto row = batch_selection_vector_.Index(cursor_position_);
    ++cursor_position_;

    for (const auto& [col_id, array] : column_id_to_array_) {
      if (array->IsNull(row)) {
        continue;
      }

      const auto& column_to_retrieve_ = columns_to_retrieve_[col_id];

      isnull[column_to_retrieve_.gp_index] = false;
      values[column_to_retrieve_.gp_index] =
          GetDatumFromArrow(column_to_retrieve_.gp_type, array, row, converter, column_to_retrieve_.substitute_unicode);
    }

    return arrow::Status::OK();
  }

  enum class Encoding { kCp1251, kUtf8 };

  void SetEncoding(Encoding enc) { encoding_ = enc; }

 private:
  struct ColumnToConvert {
    int gp_index;
    Oid gp_type;
    std::string name_in_batch;
    bool substitute_unicode;

    ColumnToConvert() = delete;
    ColumnToConvert(int other_gp_index, Oid other_gp_type, std::string other_name_in_batch,
                    bool other_substitute_unicode)
        : gp_index(other_gp_index),
          gp_type(other_gp_type),
          name_in_batch(std::move(other_name_in_batch)),
          substitute_unicode(other_substitute_unicode) {}
  };

  Encoding encoding_ = ArrowToGpConverter::Encoding::kUtf8;
  bool substitute_unicode_if_json_type_ = false;

  std::vector<ColumnToConvert> columns_to_retrieve_;

  size_t cursor_position_ = 0;
  iceberg::SelectionVector<int32_t> batch_selection_vector_{0};
  std::vector<std::pair<int32_t, std::shared_ptr<arrow::Array>>> column_id_to_array_;
};

}  // namespace tea
