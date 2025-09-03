#pragma once

#include "tea/table/numeric_var.h"

struct NumericData;

namespace tea {

#define NUMERIC_INT32_PRECISION 9
#define NUMERIC_INT64_PRECISION 18
#define NUMERIC_INT128_PRECISION 38

extern struct NumericData* NumericVarToNumeric(NumericVar* var);

}  // namespace tea
