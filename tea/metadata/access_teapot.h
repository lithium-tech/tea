#pragma once

#include <string>
#include <utility>

#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
#include "tea/observability/planner_stats.h"

namespace tea::meta::access {

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromTeapot(const Config& config, const std::string& table_name,
                                                                   const std::string& session_id, int segment_id,
                                                                   int segment_count, const std::string& filter);

}  // namespace tea::meta::access
