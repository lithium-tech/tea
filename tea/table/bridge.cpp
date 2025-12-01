#include "tea/table/bridge.h"

#include <arrow/array/array_binary.h>

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "access/formatter.h"
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"

#ifdef Abs
#undef Abs
#endif

#ifdef NIL
#undef NIL
#endif
}

#include <bit>
#include <tuple>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/decimal.h"

#include "tea/table/json_substitutor.h"
#include "tea/table/numeric.h"
#include "tea/table/numeric_var.h"

namespace tea {
namespace {

static constexpr int32_t kUuidLen = 16;
static constexpr int64_t kPostgresLagMicros = 946684800000000;
static constexpr int32_t kPostgresLagDays = 10957;

template <typename T>
auto GetFieldView(const std::shared_ptr<arrow::Array>& field, int64_t row) {
  return static_cast<const T*>(field.get())->GetView(row);
}

inline Datum StringViewToText(const std::string_view s, CharsetConverter converter) {
  return PointerGetDatum(converter.proc(converter.context, s.data(), s.size()));
}

Datum CreateDatumArray(const Oid array_oid, const std::shared_ptr<arrow::Array>& values, CharsetConverter converter,
                       bool substitute_unicode) {
  const auto get_element_type = [](const auto array_oid) -> std::tuple<Oid, int, bool, char> {
    switch (array_oid) {
      // Arithmetic.
      case BOOLARRAYOID:
        return {BOOLOID, sizeof(bool), true, 'c'};
      case INT2ARRAYOID:
        return {INT2OID, sizeof(int16_t), true, 's'};
      case INT4ARRAYOID:
        return {INT4OID, sizeof(int32_t), true, 'i'};
      case INT8ARRAYOID:
        return {INT8OID, sizeof(int64_t), true, 'd'};
      case FLOAT4ARRAYOID:
        return {FLOAT4OID, sizeof(float), true, 'i'};
      case FLOAT8ARRAYOID:
        return {FLOAT8OID, sizeof(double), true, 'd'};
      // Dates.
      case DATEARRAYOID:
        return {DATEOID, sizeof(int32_t), true, 'i'};
      case TIMEARRAYOID:
        return {TIMEOID, sizeof(int64_t), true, 'd'};
      case TIMESTAMPARRAYOID:
        return {TIMESTAMPOID, sizeof(int64_t), true, 'd'};
      case TIMESTAMPTZARRAYOID:
        return {TIMESTAMPTZOID, sizeof(int64_t), true, 'd'};
      // Textual.
      case TEXTARRAYOID:
        return {TEXTOID, -1, false, 'i'};
      // Numeric.
      case NUMERICARRAYOID:
        return {NUMERICOID, -1, false, 'i'};
      case UUIDARRAYOID:
        return {UUIDOID, kUuidLen, false, 'c'};
      case JSONARRAYOID:
        return {JSONOID, -1, false, 'i'};
      case BYTEAARRAYOID:
        return {BYTEAOID, -1, false, 'i'};
    }
    return {0, 0, false, '\0'};
  };

  const int elems_count = values->length();
  const auto& [elem_oid, elem_size, elem_by_val, elem_align] = get_element_type(array_oid);

  if (elem_oid == 0) {
    throw arrow::Status::ExecutionError("Greenplum array of type ", array_oid, " is not supported");
  }
  // Maybe empty array.
  if (elems_count == 0) {
    return PointerGetDatum(::construct_empty_array(elem_oid));
  }
  // Allocate memory for an array.
  Datum* elems = (Datum*)palloc(sizeof(Datum) * elems_count);
  bool* nulls = values->null_count() ? (bool*)palloc0(sizeof(bool) * elems_count) : (bool*)nullptr;
  // Fill values.
  if (nulls) {
    for (int i = 0; i != elems_count; ++i) {
      if (values->IsNull(i)) {
        nulls[i] = true;
      } else {
        elems[i] = GetDatumFromArrow(elem_oid, values, i, converter, substitute_unicode);
      }
    }
  } else {
    for (int i = 0; i != elems_count; ++i) {
      elems[i] = GetDatumFromArrow(elem_oid, values, i, converter, substitute_unicode);
    }
  }

  int dims[1] = {elems_count};
  int lbs[1] = {1};
  // Construct the array.
  return PointerGetDatum(
      ::construct_md_array(elems, nulls, 1, dims, lbs, elem_oid, elem_size, elem_by_val, elem_align));
}

text* CharsetConvertIdentity(void*, const char* str, size_t len) { return cstring_to_text_with_len(str, len); }

text* CharsetConverterPg(void* proc, const char* str, size_t len) {
  char* cvt = pg_custom_to_server(str, len, PG_UTF8, proc);
  if (cvt && cvt != str) {
    text* out = cstring_to_text(cvt);
    pfree(cvt);
    return out;
  }
  return cstring_to_text_with_len(str, len);
}

text* CharsetConverterIconv(void* instance, const char* str, size_t len) {
  iconv_t converter = static_cast<iconv_t>(instance);
  char* in = const_cast<char*>(str);
  size_t in_len = len;
  // Allocate the same amount of space as the input data since we only
  // support conversion from multibyte (UTF-8) to singlebyte (CP1251).
  text* out = static_cast<text*>(palloc(VARHDRSZ + len));
  char* out_ptr = VARDATA(out);
  size_t out_len = len;

  while (in_len) {
    size_t res = iconv(converter, &in, &in_len, &out_ptr, &out_len);
    if (res == static_cast<size_t>(-1)) {
      auto iconv_err = errno;
      if (iconv_err == EILSEQ && out_len && in_len) {
        *out_ptr++ = '?';
        out_len--;

        // skip a UTF-8 code point
        bool multibyte = (*in & 0x80) != 0;
        ++in;
        --in_len;
        if (multibyte) {
          while (in_len && (*in & 0xc0) == 0x80) {
            ++in;
            --in_len;
          }
        }
      } else {
        elog(ERROR, "error converting from UTF-8: %s", strerror(iconv_err));
      }
    }
  }

  SET_VARSIZE(out, out_ptr - VARDATA(out) + VARHDRSZ);
  return out;
}

}  // namespace

Datum GetDatumFromArrow(const Oid gp_type, const std::shared_ptr<arrow::Array>& array, const int64_t row,
                        CharsetConverter converter, bool substitute_unicode) {
  const auto& type = array->type();
  if (gp_type == BYTEAOID) {
    if (type->id() != arrow::Type::BINARY) {
      throw arrow::Status::ExecutionError("Internal error in tea: gp_type is bytea, but arrow::Type is not BINARY");
    }
    const auto f = GetFieldView<arrow::BinaryArray>(array, row);
    bytea* result = (bytea*)palloc(f.size() + VARHDRSZ);
    std::memcpy(VARDATA(result), f.data(), f.size());
    SET_VARSIZE(result, VARHDRSZ + f.size());
    return Datum(result);
  }

  if (gp_type == JSONOID) {
    if (type->id() != arrow::Type::STRING && type->id() != arrow::Type::BINARY) {
      throw arrow::Status::ExecutionError(
          "Internal error in tea: gp_type is json, but arrow::Type is not BINARY or STRING");
    }
    auto sv = type->id() == arrow::Type::STRING ? GetFieldView<arrow::StringArray>(array, row)
                                                : GetFieldView<arrow::BinaryArray>(array, row);
    if (substitute_unicode && NeedToSubstituteAsciiCp1251(sv)) {
      auto substituted_sv = SubstituteAsciiCp1251(std::string(sv));
      return StringViewToText(substituted_sv, converter);
    }
    return StringViewToText(sv, converter);
  }

  switch (type->id()) {
    // Arithmetic types.
    case arrow::Type::BOOL:
      return BoolGetDatum(GetFieldView<arrow::BooleanArray>(array, row));
    case arrow::Type::INT8:
      return Int8GetDatum(GetFieldView<arrow::Int8Array>(array, row));
    case arrow::Type::INT16:
      return Int16GetDatum(GetFieldView<arrow::Int16Array>(array, row));
    case arrow::Type::INT32:
      return Int32GetDatum(GetFieldView<arrow::Int32Array>(array, row));
    case arrow::Type::INT64:
      return Int64GetDatum(GetFieldView<arrow::Int64Array>(array, row));
#if 0  // There should be no way to get them
    case arrow::Type::UINT8:
      return UInt8GetDatum(GetFieldView<arrow::UInt8Array>(array, row));
    case arrow::Type::UINT16:
      return UInt16GetDatum(GetFieldView<arrow::UInt16Array>(array, row));
    case arrow::Type::UINT32:
      return UInt32GetDatum(GetFieldView<arrow::UInt32Array>(array, row));
    case arrow::Type::UINT64:
      return UInt64GetDatum(GetFieldView<arrow::UInt64Array>(array, row));
#endif
    case arrow::Type::FLOAT: {
      if (gp_type == FLOAT4OID) {
        return Float4GetDatum(GetFieldView<arrow::FloatArray>(array, row));
      } else {
        return Float8GetDatum(GetFieldView<arrow::FloatArray>(array, row));
      }
    }
    case arrow::Type::DOUBLE:
      return Float8GetDatum(GetFieldView<arrow::DoubleArray>(array, row));

    // Text types.
    case arrow::Type::STRING:
      return StringViewToText(GetFieldView<arrow::StringArray>(array, row), converter);
    case arrow::Type::BINARY:
      return StringViewToText(GetFieldView<arrow::BinaryArray>(array, row), converter);

    // UUID type.
    case arrow::Type::FIXED_SIZE_BINARY: {
      const auto f = static_cast<const arrow::FixedSizeBinaryArray*>(array.get());

      assert(f->byte_width() == kUuidLen);

      void* dest = palloc(kUuidLen);
      std::memcpy(dest, f->Value(row), kUuidLen);
      return PointerGetDatum(dest);
    }

    // Date & time types.
    case arrow::Type::DATE32:
      return DateADTGetDatum(GetFieldView<arrow::Date32Array>(array, row) - kPostgresLagDays);
    case arrow::Type::TIMESTAMP:
      return TimestampGetDatum(GetFieldView<arrow::TimestampArray>(array, row) - kPostgresLagMicros);
    case arrow::Type::TIME64:
      return Int64GetDatum(GetFieldView<arrow::Time64Array>(array, row));

    // Decimal (numeric) types.
    case arrow::Type::DECIMAL128: {
      const auto f = static_cast<const arrow::Decimal128Array*>(array.get());
      const auto t = static_cast<const arrow::Decimal128Type*>(type.get());
      NumericVar var;
      quick_init_var(&var);
      const arrow::Decimal128 decimal(f->GetValue(row));
      Int128ToNumericVar(*std::bit_cast<const Int128*>(decimal.native_endian_bytes()), t->scale(), &var);
      return NumericGetDatum(NumericVarToNumeric(&var));
    }

    // List type.
    case arrow::Type::LIST: {
      const auto& list = static_cast<const arrow::ListArray*>(array.get())->value_slice(row);

      return CreateDatumArray(gp_type, list, converter, substitute_unicode);
    }

    // Large text types.
    case arrow::Type::LARGE_STRING:
      return StringViewToText(GetFieldView<arrow::LargeStringArray>(array, row), converter);
    case arrow::Type::LARGE_BINARY:
      return StringViewToText(GetFieldView<arrow::LargeBinaryArray>(array, row), converter);

    default:
      throw arrow::Status::ExecutionError("Conversion from ", type->ToString(), " is not supported");
  }
}

bool MatchArrowColumn(const std::shared_ptr<arrow::DataType>& type, const Oid gp_type, const int gp_type_mode) {
#define MATCH_TYPE_IF(gtype, ptype, pred)                      \
  if (gp_type == (gtype) && type->id() == (ptype) && (pred)) { \
    return true;                                               \
  }

  const auto is_decimal_compatible = [gp_type_mode](auto ltype) {
    const auto dec_type = std::static_pointer_cast<const arrow::Decimal128Type>(ltype);
    const auto precision = ((gp_type_mode - 4) >> 16) & 65535;
    const auto scale = (gp_type_mode - 4) & 65535;
    return precision >= dec_type->precision() && scale == dec_type->scale();
  };

  const auto is_utc = [](const std::string& timezone) {
    return timezone == "UTC" || timezone == "Etc/UTC" || timezone == "+00:00";
  };

  const auto is_microsecond_time = [](auto ltype) {
    const auto tm_type = std::static_pointer_cast<const arrow::Time64Type>(ltype);
    return tm_type->unit() == arrow::TimeUnit::MICRO;
  };

  const auto is_microsecond_timestamp = [is_utc](auto ltype, bool is_utc_adjusted) {
    const auto ts_type = std::static_pointer_cast<const arrow::TimestampType>(ltype);
    return ts_type->unit() == arrow::TimeUnit::MICRO && (is_utc(ts_type->timezone()) == is_utc_adjusted);
  };

  const auto is_list_type_of = [gp_type_mode](auto ltype, auto oid) {
    const auto list_type = std::static_pointer_cast<const arrow::ListType>(ltype);
    return MatchArrowColumn(list_type->value_type(), oid, gp_type_mode);
  };

  // Boolean type.
  MATCH_TYPE_IF(BOOLOID, arrow::Type::BOOL, true)

  // Integral types.
  MATCH_TYPE_IF(INT2OID, arrow::Type::INT8, true)
  MATCH_TYPE_IF(INT2OID, arrow::Type::INT16, true)
  MATCH_TYPE_IF(INT2OID, arrow::Type::INT32, true)

  MATCH_TYPE_IF(INT4OID, arrow::Type::INT8, true)
  MATCH_TYPE_IF(INT4OID, arrow::Type::INT16, true)
  MATCH_TYPE_IF(INT4OID, arrow::Type::INT32, true)

  MATCH_TYPE_IF(INT8OID, arrow::Type::INT32, true)
  MATCH_TYPE_IF(INT8OID, arrow::Type::INT64, true)

  // Floating point types.
  MATCH_TYPE_IF(FLOAT4OID, arrow::Type::FLOAT, true)
  MATCH_TYPE_IF(FLOAT8OID, arrow::Type::FLOAT, true)
  MATCH_TYPE_IF(FLOAT8OID, arrow::Type::DOUBLE, true)

  // Decimal types.
  MATCH_TYPE_IF(NUMERICOID, arrow::Type::DECIMAL128, is_decimal_compatible(type))

  // Textual types.
  MATCH_TYPE_IF(BYTEAOID, arrow::Type::BINARY, true);
  MATCH_TYPE_IF(TEXTOID, arrow::Type::STRING, true)
  MATCH_TYPE_IF(JSONOID, arrow::Type::STRING, true)
  MATCH_TYPE_IF(JSONOID, arrow::Type::BINARY, true)

  // Date & time types.
  MATCH_TYPE_IF(DATEOID, arrow::Type::DATE32, true)
  MATCH_TYPE_IF(TIMEOID, arrow::Type::TIME64, is_microsecond_time(type))
  MATCH_TYPE_IF(TIMESTAMPOID, arrow::Type::TIMESTAMP, is_microsecond_timestamp(type, /* is_utc */ false))
  MATCH_TYPE_IF(TIMESTAMPTZOID, arrow::Type::TIMESTAMP, is_microsecond_timestamp(type, /* is_utc */ true))

  // UUID type.
  MATCH_TYPE_IF(UUIDOID, arrow::Type::FIXED_SIZE_BINARY, type->byte_width() == kUuidLen)

  // List types.
  MATCH_TYPE_IF(BOOLARRAYOID, arrow::Type::LIST, is_list_type_of(type, BOOLOID))
  MATCH_TYPE_IF(INT2ARRAYOID, arrow::Type::LIST, is_list_type_of(type, INT2OID))
  MATCH_TYPE_IF(INT4ARRAYOID, arrow::Type::LIST, is_list_type_of(type, INT4OID))
  MATCH_TYPE_IF(INT8ARRAYOID, arrow::Type::LIST, is_list_type_of(type, INT8OID))
  MATCH_TYPE_IF(FLOAT4ARRAYOID, arrow::Type::LIST, is_list_type_of(type, FLOAT4OID))
  MATCH_TYPE_IF(FLOAT8ARRAYOID, arrow::Type::LIST, is_list_type_of(type, FLOAT8OID))
  MATCH_TYPE_IF(DATEARRAYOID, arrow::Type::LIST, is_list_type_of(type, DATEOID))
  MATCH_TYPE_IF(TIMEARRAYOID, arrow::Type::LIST, is_list_type_of(type, TIMEOID))
  MATCH_TYPE_IF(TIMESTAMPARRAYOID, arrow::Type::LIST, is_list_type_of(type, TIMESTAMPOID))
  MATCH_TYPE_IF(TIMESTAMPTZARRAYOID, arrow::Type::LIST, is_list_type_of(type, TIMESTAMPTZOID))
  MATCH_TYPE_IF(TEXTARRAYOID, arrow::Type::LIST, is_list_type_of(type, TEXTOID))
  MATCH_TYPE_IF(NUMERICARRAYOID, arrow::Type::LIST, is_list_type_of(type, NUMERICOID))
  MATCH_TYPE_IF(UUIDARRAYOID, arrow::Type::LIST, is_list_type_of(type, UUIDOID))
  MATCH_TYPE_IF(JSONARRAYOID, arrow::Type::LIST, is_list_type_of(type, JSONOID))
  MATCH_TYPE_IF(JSONARRAYOID, arrow::Type::LIST, is_list_type_of(type, TEXTOID))
  MATCH_TYPE_IF(BYTEAARRAYOID, arrow::Type::LIST, is_list_type_of(type, BYTEAOID))

#undef MATCH_TYPE_IF

  return false;
}

std::optional<FieldId> MatchIcebergColumn(const iceberg::types::NestedField& field, const Oid gp_type) {
  using IceTypeId = iceberg::TypeID;

  FieldId field_id = field.field_id;
  if (field.type->TypeId() == IceTypeId::kUnknown) return true;

#define MATCH(gtype, itype)                                    \
  if (gp_type == (gtype) && field.type->TypeId() == (itype)) { \
    return field_id;                                           \
  }

  MATCH(BOOLOID, IceTypeId::kBoolean)
  MATCH(INT2OID, IceTypeId::kInt)
  MATCH(INT4OID, IceTypeId::kInt)
  MATCH(TIMEOID, IceTypeId::kTime)
  MATCH(DATEOID, IceTypeId::kDate)
  MATCH(INT8OID, IceTypeId::kLong)
  MATCH(TIMESTAMPOID, IceTypeId::kTimestamp)
  MATCH(TIMESTAMPTZOID, IceTypeId::kTimestamptz)
  MATCH(FLOAT4OID, IceTypeId::kFloat)
  MATCH(FLOAT8OID, IceTypeId::kDouble)
  MATCH(UUIDOID, IceTypeId::kUuid)
  MATCH(NUMERICOID, IceTypeId::kDecimal)
  MATCH(TEXTOID, IceTypeId::kString)
  MATCH(JSONOID, IceTypeId::kString)
  MATCH(BYTEAOID, IceTypeId::kBinary)
#undef MATCH

#define MATCH_ARRAY(gtype, itype)                                                                                 \
  if (gp_type == (gtype) && field.type->IsListType() &&                                                           \
      std::static_pointer_cast<const iceberg::types::ListType>(field.type)->ElementType()->TypeId() == (itype)) { \
    return field_id;                                                                                              \
  }

  MATCH_ARRAY(BOOLARRAYOID, IceTypeId::kBoolean);
  MATCH_ARRAY(INT2ARRAYOID, IceTypeId::kInt);
  MATCH_ARRAY(INT4ARRAYOID, IceTypeId::kInt);
  MATCH_ARRAY(INT8ARRAYOID, IceTypeId::kLong);
  MATCH_ARRAY(FLOAT4ARRAYOID, IceTypeId::kFloat);
  MATCH_ARRAY(FLOAT8ARRAYOID, IceTypeId::kDouble);
  MATCH_ARRAY(TIMEARRAYOID, IceTypeId::kTime);
  MATCH_ARRAY(DATEARRAYOID, IceTypeId::kDate);
  MATCH_ARRAY(TIMESTAMPARRAYOID, IceTypeId::kTimestamp);
  MATCH_ARRAY(TIMESTAMPTZARRAYOID, IceTypeId::kTimestamptz);
  MATCH_ARRAY(NUMERICARRAYOID, IceTypeId::kDecimal);
  MATCH_ARRAY(TEXTARRAYOID, IceTypeId::kString)
  MATCH_ARRAY(UUIDARRAYOID, IceTypeId::kUuid)
  MATCH_ARRAY(JSONARRAYOID, IceTypeId::kString)
  MATCH_ARRAY(BYTEAARRAYOID, IceTypeId::kBinary)

#undef MATCH_ARRAY

  return std::nullopt;
}

CharsetConverter MakeIdentityConverter() { return {.proc = CharsetConvertIdentity}; }

CharsetConverter MakePgConverter(FmgrInfo* converter_proc) {
  return {.proc = CharsetConverterPg, .context = converter_proc};
}

CharsetConverter MakeIconvConverter(iconv_t instance) { return {.proc = CharsetConverterIconv, .context = instance}; }

arrow::Result<iconv_t> InitializeIconv(int db_encoding) {
  const char* dest_encoding;
  switch (db_encoding) {
    case PG_UTF8:
      return nullptr;
    case PG_WIN1251:
      dest_encoding = "CP1251";
      break;
    default:
      return arrow::Status::ExecutionError("Unsupported database encoding ", db_encoding);
  }
  iconv_t enc = iconv_open(dest_encoding, "UTF-8");
  if (enc == reinterpret_cast<iconv_t>(-1)) {
    return arrow::Status::ExecutionError("Failed to initialize charset converter ", strerror(errno));
  }
  return enc;
}

arrow::Status FinalizeIconv(iconv_t enc) {
  int status = iconv_close(enc);
  if (status == static_cast<int>(-1)) {
    return arrow::Status::ExecutionError("Failed to deinitialize charset converter ", strerror(errno));
  }
  return arrow::Status::OK();
}

}  // namespace tea
