#pragma once

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "utils/array.h"
}

#include "arrow/result.h"
#include "iceberg/filter/representation/value.h"

#include "tea/filter/gp/types_mapping.h"

namespace tea {

class PGArray {
 public:
  static arrow::Result<PGArray> Make(ArrayType* arr, Oid array_type);

  arrow::Result<iceberg::filter::ValueType> GetValueType() const;

  size_t Size() const { return length_; }

  std::optional<Datum> operator[](size_t index) const;

 private:
  PGArray(ArrayType* arr, ElemInfo elem_info);

  Datum* datum_;
  bool* is_null_;
  size_t length_;
  ElemInfo element_info_;
};

}  // namespace tea
