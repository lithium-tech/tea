#pragma once

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "catalog/pg_type.h"

#ifdef Abs
#undef Abs
#endif

#ifdef NIL
#undef NIL
#endif

#ifdef Min
#undef Min
#endif

#ifdef Max
#undef Max
#endif
}
