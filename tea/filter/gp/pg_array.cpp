#include "tea/filter/gp/pg_array.h"

namespace tea {

arrow::Result<PGArray> PGArray::Make(ArrayType* arr, Oid array_type) {
  auto maybe_elem_info = ArrayOidToElemInfo(array_type);
  if (!maybe_elem_info.has_value()) {
    return arrow::Status::ExecutionError("Tea error: Using unsupported data type (", array_type, ")");
  }
  return PGArray(arr, maybe_elem_info.value());
}

arrow::Result<iceberg::filter::ValueType> PGArray::GetValueType() const {
  auto maybe_var_type = tea::TypeOidToValueType(element_info_.type);
  if (!maybe_var_type.has_value()) {
    return arrow::Status::NotImplemented("ConvertConstArray: internal error in tea. Unexpected oid ",
                                         element_info_.type);
  }
  return maybe_var_type.value();
}

std::optional<Datum> PGArray::operator[](size_t index) const {
  if (index >= length_) {
    throw std::runtime_error("Internal error in tea (PGArray operator[])");
  }
  if (!is_null_[index]) {
    return datum_[index];
  } else {
    return std::nullopt;
  }
}

PGArray::PGArray(ArrayType* arr, ElemInfo elem_info) : element_info_(elem_info) {
  int32_t len = -1;
  deconstruct_array(arr, elem_info.type, elem_info.length_in_bytes, elem_info.by_value, elem_info.alignment, &datum_,
                    &is_null_, &len);
  if (len < 0) {
    throw std::runtime_error("Internal error in tea (PGArray constructor)");
  }
  length_ = len;
}

}  // namespace tea
