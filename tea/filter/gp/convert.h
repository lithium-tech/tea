#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"  // NOLINT build/include_subdir

#include "access/tupdesc.h"
#include "nodes/pg_list.h"

#include "tea/filter/gp/serialized_filter.h"

typedef enum TeaTableType {
  kExternal,
  kForeign,
} TeaTableType;

CSerializedFilter ConvertPGClausesToTeaNodes(TeaTableType table_type, List* input_clauses, List** converted_clauses,
                                             List** not_converted_clauses, TupleDesc tuple_desc,
                                             IgnoredExprs ignored_exprs);

#ifdef __cplusplus
}
#endif
