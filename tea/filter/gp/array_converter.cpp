#include "tea/filter/gp/array_converter.h"

#include "tea/filter/gp/common.h"

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
}

namespace tea {
namespace {

using ValueType = iceberg::filter::ValueType;
template <ValueType value_type>
using Array = iceberg::filter::Array<value_type>;

template <ValueType value_type>
Array<value_type> ConvertArray(const PGArray& array,
                               const std::function<iceberg::filter::PhysicalType<value_type>(const Datum&)>& func) {
  Array<value_type> result(array.Size());
  for (size_t index = 0; index < array.Size(); ++index) {
    if (auto maybe_datum = array[index]; maybe_datum.has_value()) {
      result[index] = func(maybe_datum.value());
    }
  }
  return result;
}

}  // namespace

arrow::Result<iceberg::filter::ArrayHolder> ArrayConverter::Convert(const PGArray& array,
                                                                    const StringConverter& string_converter) {
  using iceberg::filter::ArrayHolder;
  using iceberg::filter::ValueType;

  ARROW_ASSIGN_OR_RAISE(ValueType value_type, array.GetValueType())
  switch (value_type) {
    case ValueType::kBool:
      return ArrayHolder::Make<ValueType::kBool>(ArrayConverter::ConvertBoolArray(array));
    case ValueType::kInt2:
      return ArrayHolder::Make<ValueType::kInt2>(ArrayConverter::ConvertInt2Array(array));
    case ValueType::kInt4:
      return ArrayHolder::Make<ValueType::kInt4>(ArrayConverter::ConvertInt4Array(array));
    case ValueType::kInt8:
      return ArrayHolder::Make<ValueType::kInt8>(ArrayConverter::ConvertInt8Array(array));
    case ValueType::kFloat4:
      return ArrayHolder::Make<ValueType::kFloat4>(ArrayConverter::ConvertFloat4Array(array));
    case ValueType::kFloat8:
      return ArrayHolder::Make<ValueType::kFloat8>(ArrayConverter::ConvertFloat8Array(array));
    case ValueType::kNumeric:
      return ArrayHolder::Make<ValueType::kNumeric>(ArrayConverter::ConvertNumericArray(array));
    case ValueType::kString:
      return ArrayHolder::Make<ValueType::kString>(ArrayConverter::ConvertStringArray(array, string_converter));
    case ValueType::kDate:
      return ArrayHolder::Make<ValueType::kDate>(ArrayConverter::ConvertDateArray(array));
    case ValueType::kTimestamp:
      return ArrayHolder::Make<ValueType::kTimestamp>(ArrayConverter::ConvertTimestampArray(array));
    case ValueType::kTimestamptz:
      return ArrayHolder::Make<ValueType::kTimestamptz>(ArrayConverter::ConvertTimestamptzArray(array));
    case ValueType::kTime:
      return ArrayHolder::Make<ValueType::kTime>(ArrayConverter::ConvertTimeArray(array));
    case ValueType::kInterval:
      return arrow::Status::NotImplemented("Array of interval is not supported");
  }
  return arrow::Status::ExecutionError("Internal error in tea. ", __PRETTY_FUNCTION__);
}

Array<ValueType::kBool> ArrayConverter::ConvertBoolArray(const PGArray& array) {
  return ConvertArray<ValueType::kBool>(array, DatumGetBool);
}

Array<ValueType::kInt2> ArrayConverter::ConvertInt2Array(const PGArray& array) {
  return ConvertArray<ValueType::kInt2>(array, DatumGetInt16);
}

Array<ValueType::kInt4> ArrayConverter::ConvertInt4Array(const PGArray& array) {
  return ConvertArray<ValueType::kInt4>(array, DatumGetInt32);
}

Array<ValueType::kInt8> ArrayConverter::ConvertInt8Array(const PGArray& array) {
  return ConvertArray<ValueType::kInt8>(array, DatumGetInt64);
}

Array<ValueType::kFloat4> ArrayConverter::ConvertFloat4Array(const PGArray& array) {
  return ConvertArray<ValueType::kFloat4>(array, DatumGetFloat4);
}

Array<ValueType::kFloat8> ArrayConverter::ConvertFloat8Array(const PGArray& array) {
  return ConvertArray<ValueType::kFloat8>(array, DatumGetFloat8);
}

Array<ValueType::kNumeric> ArrayConverter::ConvertNumericArray(const PGArray& array) {
  Oid typoutput;
  bool typIsVarlena;
  getTypeOutputInfo(NUMERICOID, &typoutput, &typIsVarlena);
  return ConvertArray<ValueType::kNumeric>(array, [typoutput](const Datum& data) {
    return iceberg::filter::Numeric{.value = OidOutputFunctionCall(typoutput, data)};
  });
}

Array<ValueType::kString> ArrayConverter::ConvertStringArray(const PGArray& array,
                                                             const StringConverter& string_converter) {
  return ConvertArray<ValueType::kString>(array, [&string_converter](const Datum& data) {
    char* value = DatumGetCString(DirectFunctionCall1(textout, data));
    return string_converter(value, strlen(value));
  });
}

Array<ValueType::kTimestamp> ArrayConverter::ConvertTimestampArray(const PGArray& array) {
  return ConvertArray<ValueType::kTimestamp>(array, DatumGetUnixTimestampMicros);
}

Array<ValueType::kTimestamptz> ArrayConverter::ConvertTimestamptzArray(const PGArray& array) {
  return ConvertArray<ValueType::kTimestamptz>(array, DatumGetUnixTimestampMicros);
}

Array<ValueType::kDate> ArrayConverter::ConvertDateArray(const PGArray& array) {
  return ConvertArray<ValueType::kDate>(array, DatumGetUnixDate);
}

Array<ValueType::kTime> ArrayConverter::ConvertTimeArray(const PGArray& array) {
  return ConvertArray<ValueType::kTime>(array, [](const Datum& data) { return DatumGetTimeADT(data); });
}

}  // namespace tea
