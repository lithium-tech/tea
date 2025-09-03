#include "tea/table/gp_funcs.h"

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "utils/builtins.h"
}

namespace tea {

std::string FormatTypeWithTypeMode(Oid oid, int32_t type_mode) { return format_type_with_typemod(oid, type_mode); }

}  // namespace tea
