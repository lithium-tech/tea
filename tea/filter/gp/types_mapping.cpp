#include "tea/filter/gp/types_mapping.h"

#include <algorithm>
#include <vector>

#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/value.h"

#include "tea/table/gp_fwd.h"

extern "C" {
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
}

namespace tea {

using iceberg::filter::ValueType;

struct OidToValueType {
  Oid oid;
  ValueType value_type;
};

constexpr OidToValueType kOidToValueTypeMap[] = {{BOOLOID, ValueType::kBool},
                                                 {INT2OID, ValueType::kInt2},
                                                 {INT4OID, ValueType::kInt4},
                                                 {INT8OID, ValueType::kInt8},
#if 0
                                                 {FLOAT4OID, ValueType::kFloat4},
                                                 {FLOAT8OID, ValueType::kFloat8},
#endif
                                                 {NUMERICOID, ValueType::kNumeric},
                                                 {TEXTOID, ValueType::kString},
                                                 {DATEOID, ValueType::kDate},
                                                 {TIMESTAMPOID, ValueType::kTimestamp},
                                                 {TIMESTAMPTZOID, ValueType::kTimestamptz},
                                                 {TIMEOID, ValueType::kTime},
                                                 {INTERVALOID, ValueType::kInterval}};

std::optional<ValueType> TypeOidToValueType(Oid oid) {
  auto iter = std::find_if(std::begin(kOidToValueTypeMap), std::end(kOidToValueTypeMap),
                           [oid](const auto& entry) { return entry.oid == oid; });
  if (iter == std::end(kOidToValueTypeMap)) {
    return std::nullopt;
  }
  return iter->value_type;
}

struct OidToElemInfo {
  Oid array_oid;
  ElemInfo element;
};

constexpr OidToElemInfo kOidToElemInfoMap[] = {{BOOLARRAYOID, {BOOLOID, sizeof(bool), true, 'c'}},
                                               {INT2ARRAYOID, {INT2OID, sizeof(int16), true, 's'}},
                                               {INT4ARRAYOID, {INT4OID, sizeof(int32), true, 'i'}},
                                               {INT8ARRAYOID, {INT8OID, sizeof(int64), true, 'd'}},
#if 0
                                               {FLOAT4ARRAYOID, {FLOAT4OID, sizeof(float4), true, 'i'}},
                                               {FLOAT8ARRAYOID, {FLOAT8OID, sizeof(float8), true, 'd'}},
#endif
                                               {NUMERICARRAYOID, {NUMERICOID, -1, false, 'i'}},
                                               {TEXTARRAYOID, {TEXTOID, -1, false, 'i'}},
                                               {TIMESTAMPARRAYOID, {TIMESTAMPOID, sizeof(int64), true, 'd'}},
                                               {TIMESTAMPTZARRAYOID, {TIMESTAMPTZOID, sizeof(int64), true, 'd'}},
                                               {DATEARRAYOID, {DATEOID, sizeof(int32), true, 'i'}},
                                               {TIMEARRAYOID, {TIMEOID, sizeof(int64), true, 'd'}}
                                               /*TODO(gmusya): support intervalarray */};

std::optional<ElemInfo> ArrayOidToElemInfo(Oid arrayoid) {
  auto iter = std::find_if(std::begin(kOidToElemInfoMap), std::end(kOidToElemInfoMap),
                           [arrayoid](const auto& elem) { return elem.array_oid == arrayoid; });
  if (iter == std::end(kOidToElemInfoMap)) {
    return std::nullopt;
  }
  return iter->element;
}

struct OidToFunctionSignature {
  Oid oid;
  iceberg::filter::FunctionSignature signature;
};

std::optional<iceberg::filter::FunctionSignature> FuncOidToFunctionSignature(Oid oid) {
  using FS = iceberg::filter::FunctionSignature;
  using VT = ValueType;
  using ID = iceberg::filter::FunctionID;
  using O2FS = OidToFunctionSignature;

  // oids are taken from catalog/pg_operator.h, catalog/pg_proc.h, catalog/pg_proc_gp.h
  static const std::vector<OidToFunctionSignature> kSupportedFunctions = []() {
    // TODO(gmusya): make constexpr
    std::vector<OidToFunctionSignature> result;

#if 0
    result.emplace_back(O2FS{311, FS{ID::kCastFloat8, VT::kFloat8, {VT::kFloat4}}});
#endif
    result.emplace_back(O2FS{313, FS{ID::kCastInt4, VT::kInt4, {VT::kInt2}}});

    result.emplace_back(O2FS{481, FS{ID::kCastInt8, VT::kInt8, {VT::kInt4}}});

    result.emplace_back(O2FS{754, FS{ID::kCastInt8, VT::kInt8, {VT::kInt2}}});

    result.emplace_back(O2FS{849, FS{ID::kLocate, VT::kInt4, {VT::kString, VT::kString}}});
    result.emplace_back(O2FS{870, FS{ID::kLower, VT::kString, {VT::kString}}});
    result.emplace_back(O2FS{871, FS{ID::kUpper, VT::kString, {VT::kString}}});
    result.emplace_back(O2FS{875, FS{ID::kLTrim, VT::kString, {VT::kString, VT::kString}}});
    result.emplace_back(O2FS{876, FS{ID::kRTrim, VT::kString, {VT::kString, VT::kString}}});
    result.emplace_back(O2FS{877, FS{ID::kSubstring, VT::kString, {VT::kString, VT::kInt4, VT::kInt4}}});
    result.emplace_back(O2FS{883, FS{ID::kSubstring, VT::kString, {VT::kString, VT::kInt4}}});
    result.emplace_back(O2FS{884, FS{ID::kBTrim, VT::kString, {VT::kString, VT::kString}}});

    result.emplace_back(O2FS{936, FS{ID::kSubstring, VT::kString, {VT::kString, VT::kInt4, VT::kInt4}}});
    result.emplace_back(O2FS{937, FS{ID::kSubstring, VT::kString, {VT::kString, VT::kInt4}}});

    result.emplace_back(O2FS{1178, FS{ID::kCastDate, VT::kDate, {VT::kTimestamptz}}});

    result.emplace_back(O2FS{1317, FS{ID::kCharLength, VT::kInt4, {VT::kString}}});
    result.emplace_back(O2FS{1381, FS{ID::kCharLength, VT::kInt4, {VT::kString}}});

#if 0
    result.emplace_back(O2FS{1340, FS{ID::kLog10, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1342, FS{ID::kRound, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1344, FS{ID::kSqrt, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1345, FS{ID::kCbrt, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1347, FS{ID::kExp, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1394, FS{ID::kAbsolute, VT::kFloat4, {VT::kFloat4}}});
    result.emplace_back(O2FS{1395, FS{ID::kAbsolute, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1396, FS{ID::kAbsolute, VT::kInt8, {VT::kInt8}}});
    result.emplace_back(O2FS{1397, FS{ID::kAbsolute, VT::kInt4, {VT::kInt4}}});

    result.emplace_back(O2FS{1600, FS{ID::kAsin, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1601, FS{ID::kAcos, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1602, FS{ID::kAtan, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1603, FS{ID::kAtan2, VT::kFloat8, {VT::kFloat8, VT::kFloat8}}});
    result.emplace_back(O2FS{1604, FS{ID::kSin, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1605, FS{ID::kCos, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1606, FS{ID::kTan, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{1607, FS{ID::kCot, VT::kFloat8, {VT::kFloat8}}});
#endif

    result.emplace_back(O2FS{2020, FS{ID::kDateTrunc, VT::kInt4, {VT::kString, VT::kTimestamp}}});
    result.emplace_back(O2FS{2021, FS{ID::kExtractTimestamp, VT::kInt4, {VT::kString, VT::kTimestamp}}});
    result.emplace_back(O2FS{2027, FS{ID::kCastTimestamp, VT::kTimestamp, {VT::kTimestamptz}}});
    result.emplace_back(O2FS{2028, FS{ID::kCastTimestamptz, VT::kTimestamptz, {VT::kTimestamp}}});
    result.emplace_back(O2FS{2029, FS{ID::kCastDate, VT::kDate, {VT::kTimestamp}}});

    result.emplace_back(O2FS{2308, FS{ID::kCeil, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{2309, FS{ID::kFloor, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{2310, FS{ID::kSign, VT::kFloat8, {VT::kFloat8}}});

    result.emplace_back(O2FS{7539, FS{ID::kCosh, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{7540, FS{ID::kSinh, VT::kFloat8, {VT::kFloat8}}});
    result.emplace_back(O2FS{7541, FS{ID::kTanh, VT::kFloat8, {VT::kFloat8}}});

    return result;
  }();

  if (auto it = std::find_if(std::begin(kSupportedFunctions), std::end(kSupportedFunctions),
                             [oid](const auto& elem) { return elem.oid == oid; });
      it != kSupportedFunctions.end()) {
    return it->signature;
  }
  return std::nullopt;
}

std::optional<iceberg::filter::FunctionSignature> OpExprOidToFunctionSignature(Oid oid) {
  using FS = iceberg::filter::FunctionSignature;
  using VT = ValueType;
  using ID = iceberg::filter::FunctionID;
  using O2FS = OidToFunctionSignature;

  static const std::vector<OidToFunctionSignature> kSupportedFunctions = []() {
    // TODO(gmusya): make constexpr
    std::vector<OidToFunctionSignature> result;
    {
      auto append_comparisons = [&](Oid eq, Oid lt, Oid gt, Oid le, Oid ge, Oid ne, ValueType lhs, ValueType rhs) {
        result.emplace_back(O2FS{eq, FS{ID::kEqual, VT::kBool, {lhs, rhs}}});
        result.emplace_back(O2FS{lt, FS{ID::kLessThan, VT::kBool, {lhs, rhs}}});
        result.emplace_back(O2FS{gt, FS{ID::kGreaterThan, VT::kBool, {lhs, rhs}}});
        result.emplace_back(O2FS{le, FS{ID::kLessThanOrEqualTo, VT::kBool, {lhs, rhs}}});
        result.emplace_back(O2FS{ge, FS{ID::kGreaterThanOrEqualTo, VT::kBool, {lhs, rhs}}});
        result.emplace_back(O2FS{ne, FS{ID::kNotEqual, VT::kBool, {lhs, rhs}}});
      };

      append_comparisons(BooleanEqualOperator, 58, 59, 1694, 1695, 85, VT::kBool, VT::kBool);
      append_comparisons(Int2EqualOperator, 95, 520, 522, 524, 519, VT::kInt2, VT::kInt2);
      append_comparisons(Int4EqualOperator, 97, 521, 523, 525, 518, VT::kInt4, VT::kInt4);
      append_comparisons(Int8EqualOperator, 412, 413, 414, 415, 411, VT::kInt8, VT::kInt8);
      append_comparisons(Int24EqualOperator, 534, 536, 540, 542, 538, VT::kInt2, VT::kInt4);
      append_comparisons(Int42EqualOperator, 535, 537, 541, 543, 539, VT::kInt4, VT::kInt2);
      append_comparisons(Int28EqualOperator, 1864, 1865, 1866, 1867, 1863, VT::kInt2, VT::kInt8);
      append_comparisons(Int82EqualOperator, 1870, 1871, 1872, 1873, 1869, VT::kInt8, VT::kInt2);
      append_comparisons(Int48EqualOperator, 37, 76, 80, 82, 36, VT::kInt4, VT::kInt8);
      append_comparisons(Int84EqualOperator, 418, 419, 420, 430, 417, VT::kInt8, VT::kInt4);
#if 0
      append_comparisons(Float4EqualOperator, 622, 623, 624, 625, 621, VT::kFloat4, VT::kFloat4);
      append_comparisons(Float8EqualOperator, 672, 674, 673, 675, 671, VT::kFloat8, VT::kFloat8);
      append_comparisons(Float48EqualOperator, 1122, 1123, 1124, 1125, 1121, VT::kFloat4, VT::kFloat8);
      append_comparisons(Float84EqualOperator, 1132, 1133, 1134, 1135, 1131, VT::kFloat8, VT::kFloat4);
#endif
      append_comparisons(NumericEqualOperator, 1754, 1756, 1755, 1757, 1753, VT::kNumeric, VT::kNumeric);
      append_comparisons(TextEqualOperator, 664, 666, 665, 667, 531, VT::kString, VT::kString);
      append_comparisons(DateEqualOperator, 1095, 1097, 1096, 1098, 1094, VT::kDate, VT::kDate);
      append_comparisons(TimestampEqualOperator, 2062, 2064, 2063, 2065, 2061, VT::kTimestamp, VT::kTimestamp);
      append_comparisons(TimestampTZEqualOperator, 1322, 1324, 1323, 1325, 1321, VT::kTimestamptz, VT::kTimestamptz);
      append_comparisons(TimeEqualOperator, 1110, 1112, 1111, 1113, 1109, VT::kTime, VT::kTime);
      append_comparisons(2536, 2534, 2538, 2535, 2537, 2539, VT::kTimestamp, VT::kTimestamptz);
      append_comparisons(2542, 2540, 2544, 2541, 2543, 2545, VT::kTimestamptz, VT::kTimestamp);
      append_comparisons(2347, 2345, 2349, 2346, 2348, 2350, VT::kDate, VT::kTimestamp);
      append_comparisons(2373, 2371, 2375, 2372, 2374, 2376, VT::kTimestamp, VT::kDate);
      append_comparisons(2360, 2358, 2362, 2359, 2361, 2363, VT::kDate, VT::kTimestamptz);
      append_comparisons(2386, 2384, 2388, 2385, 2387, 2389, VT::kTimestamptz, VT::kDate);
    }
    {
      auto append_integer_arithmetic = [&](Oid add, Oid sub, Oid mul, Oid div, Oid mod, ValueType res_type,
                                           ValueType lhs, ValueType rhs) {
        if (add != InvalidOid) {
          result.emplace_back(O2FS{add, FS{ID::kAddWithChecks, res_type, {lhs, rhs}}});
        }
        if (sub != InvalidOid) {
          result.emplace_back(O2FS{sub, FS{ID::kSubtractWithChecks, res_type, {lhs, rhs}}});
        }
        if (mul != InvalidOid) {
          result.emplace_back(O2FS{mul, FS{ID::kMultiplyWithChecks, res_type, {lhs, rhs}}});
        }
        if (div != InvalidOid) {
          result.emplace_back(O2FS{div, FS{ID::kDivideWithChecks, res_type, {lhs, rhs}}});
        }
        if (mod != InvalidOid) {
          result.emplace_back(O2FS{mod, FS{ID::kModuloWithChecks, res_type, {lhs, rhs}}});
        }
      };

      append_integer_arithmetic(550, 554, 526, 527, 529, VT::kInt2, VT::kInt2, VT::kInt2);
      append_integer_arithmetic(551, 555, 514, 528, 530, VT::kInt4, VT::kInt4, VT::kInt4);
      append_integer_arithmetic(684, 685, 686, 687, 439, VT::kInt8, VT::kInt8, VT::kInt8);
      append_integer_arithmetic(552, 556, 544, 546, InvalidOid, VT::kInt4, VT::kInt2, VT::kInt4);
      append_integer_arithmetic(553, 557, 545, 547, InvalidOid, VT::kInt4, VT::kInt4, VT::kInt2);
      append_integer_arithmetic(818, 819, 820, 821, InvalidOid, VT::kInt8, VT::kInt8, VT::kInt2);
      append_integer_arithmetic(822, 823, 824, 825, InvalidOid, VT::kInt8, VT::kInt2, VT::kInt8);
      append_integer_arithmetic(692, 693, 694, 695, InvalidOid, VT::kInt8, VT::kInt4, VT::kInt8);
      append_integer_arithmetic(688, 689, 690, 691, InvalidOid, VT::kInt8, VT::kInt8, VT::kInt4);
    }
    {
#if 0
      auto append_float_arithmetic = [&](Oid add, Oid sub, Oid mul, Oid div, ValueType res_type, ValueType lhs,
                                         ValueType rhs) {
        result.emplace_back(O2FS{add, FS{ID::kAddWithoutChecks, res_type, {lhs, rhs}}});
        result.emplace_back(O2FS{sub, FS{ID::kSubtractWithoutChecks, res_type, {lhs, rhs}}});
        result.emplace_back(O2FS{mul, FS{ID::kMultiplyWithoutChecks, res_type, {lhs, rhs}}});
        result.emplace_back(O2FS{div, FS{ID::kDivideWithoutChecks, res_type, {lhs, rhs}}});
      };
      append_float_arithmetic(586, 587, 589, 588, VT::kFloat4, VT::kFloat4, VT::kFloat4);
      append_float_arithmetic(591, 592, 594, 593, VT::kFloat8, VT::kFloat8, VT::kFloat8);
      append_float_arithmetic(1116, 1117, 1119, 1118, VT::kFloat8, VT::kFloat4, VT::kFloat8);
      append_float_arithmetic(1126, 1127, 1129, 1128, VT::kFloat8, VT::kFloat8, VT::kFloat4);
#endif
    }
    {
      auto append_bitwise = [&](Oid bit_and, Oid bit_or, Oid bit_xor, Oid bit_not, ValueType val_type) {
        result.emplace_back(O2FS{bit_and, FS{ID::kBitwiseAnd, val_type, {val_type, val_type}}});
        result.emplace_back(O2FS{bit_or, FS{ID::kBitwiseOr, val_type, {val_type, val_type}}});
        result.emplace_back(O2FS{bit_xor, FS{ID::kXor, val_type, {val_type, val_type}}});
        result.emplace_back(O2FS{bit_not, FS{ID::kBitwiseNot, val_type, {val_type}}});
      };

      append_bitwise(1880, 1881, 1882, 1883, VT::kInt4);
      append_bitwise(1886, 1887, 1888, 1889, VT::kInt8);
    }
    {
      auto append_unary = [&](Oid abs_oid, Oid neg_oid, ValueType val_type) {
        result.emplace_back(O2FS{abs_oid, FS{ID::kAbsolute, val_type, {val_type}}});
        result.emplace_back(O2FS{neg_oid, FS{ID::kNegative, val_type, {val_type}}});
      };

      append_unary(773, 558, VT::kInt4);
      append_unary(473, 484, VT::kInt8);
#if 0
      append_unary(590, 584, VT::kFloat4);
      append_unary(595, 585, VT::kFloat8);
#endif
      append_unary(1763, 1751, VT::kNumeric);
    }

    result.emplace_back(O2FS{OID_TEXT_LIKE_OP, FS{ID::kLike, VT::kBool, {VT::kString, VT::kString}}});
    result.emplace_back(O2FS{1210, FS{ID::kNotLike, VT::kBool, {VT::kString, VT::kString}}});
    result.emplace_back(O2FS{OID_TEXT_ICLIKE_OP, FS{ID::kILike, VT::kBool, {VT::kString, VT::kString}}});
    result.emplace_back(O2FS{1628, FS{ID::kNotILike, VT::kBool, {VT::kString, VT::kString}}});

    result.emplace_back(O2FS{654, FS{ID::kConcatenate, VT::kString, {VT::kString, VT::kString}}});

    result.emplace_back(O2FS{2066, FS{ID::kAddWithChecks, VT::kTimestamp, {VT::kTimestamp, VT::kInterval}}});
    result.emplace_back(O2FS{1099, FS{ID::kDateDiff, VT::kInt4, {VT::kDate, VT::kDate}}});

    return result;
  }();

  if (auto it = std::find_if(std::begin(kSupportedFunctions), std::end(kSupportedFunctions),
                             [oid](const auto& elem) { return elem.oid == oid; });
      it != kSupportedFunctions.end()) {
    return it->signature;
  }
  return std::nullopt;
}

}  // namespace tea
