#pragma once

#include <string>

#include "tea/table/gp_fwd.h"

namespace tea {

std::string FormatTypeWithTypeMode(Oid oid, int32_t type_mode);

}  // namespace tea
