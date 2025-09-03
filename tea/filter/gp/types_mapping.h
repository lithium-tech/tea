#pragma once

#include <optional>

#include "iceberg/filter/representation/function.h"

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir
}

namespace tea {

struct ElemInfo {
  Oid type;
  int32_t length_in_bytes;
  bool by_value;
  char alignment;
};

std::optional<iceberg::filter::ValueType> TypeOidToValueType(Oid oid);
std::optional<iceberg::filter::FunctionSignature> FuncOidToFunctionSignature(Oid oid);
std::optional<iceberg::filter::FunctionSignature> OpExprOidToFunctionSignature(Oid oid);
std::optional<ElemInfo> ArrayOidToElemInfo(Oid arrayoid);

}  // namespace tea
