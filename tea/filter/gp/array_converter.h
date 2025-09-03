#pragma once

#include <functional>
#include <string>

#include "arrow/result.h"
#include "iceberg/filter/representation/value.h"

#include "tea/filter/gp/pg_array.h"

namespace tea {

class ArrayConverter {
  using ValueType = iceberg::filter::ValueType;

  template <ValueType value_type>
  using Array = iceberg::filter::Array<value_type>;

  using StringConverter = std::function<std::string(const char*, int)>;

 public:
  static arrow::Result<iceberg::filter::ArrayHolder> Convert(const PGArray& array,
                                                             const StringConverter& string_converter);

 private:
  static Array<ValueType::kBool> ConvertBoolArray(const PGArray& array);
  static Array<ValueType::kInt2> ConvertInt2Array(const PGArray& array);
  static Array<ValueType::kInt4> ConvertInt4Array(const PGArray& array);
  static Array<ValueType::kInt8> ConvertInt8Array(const PGArray& array);
  static Array<ValueType::kFloat4> ConvertFloat4Array(const PGArray& array);
  static Array<ValueType::kFloat8> ConvertFloat8Array(const PGArray& array);
  static Array<ValueType::kNumeric> ConvertNumericArray(const PGArray& array);
  static Array<ValueType::kString> ConvertStringArray(const PGArray& array, const StringConverter& string_converter);
  static Array<ValueType::kTimestamp> ConvertTimestampArray(const PGArray& array);
  static Array<ValueType::kTimestamptz> ConvertTimestamptzArray(const PGArray& array);
  static Array<ValueType::kDate> ConvertDateArray(const PGArray& array);
  static Array<ValueType::kTime> ConvertTimeArray(const PGArray& array);
};

}  // namespace tea
