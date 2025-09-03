#pragma once

#include <string>

#include "iceberg/filter/representation/node.h"

namespace tea::filter {

struct TeapotFileFilterContext {
  int64_t timestamp_to_timestamptz_shift_us_;
};

std::string GetTeapotFileFilter(iceberg::filter::NodePtr root, const TeapotFileFilterContext& ctx);

}  // namespace tea::filter
