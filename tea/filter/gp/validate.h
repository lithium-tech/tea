#pragma once

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "access/tupdesc.h"
}

#include "arrow/status.h"
#include "iceberg/filter/representation/node.h"

namespace tea {

arrow::Status ValidateRowFilterClause(iceberg::filter::NodePtr expr, TupleDesc tuple_desc);

}
