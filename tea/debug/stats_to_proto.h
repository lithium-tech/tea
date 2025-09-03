#pragma once

#include <memory>
#include <string>

#include "tea/debug/stats_state.grpc.pb.h"
#include "tea/debug/stats_state.pb.h"
#include "tea/observability/reader_stats.h"
#include "tea/observability/s3_stats.h"

namespace tea::debug {

void SendStats(DurationTicks duration_ticks, double ticks_per_second, const PlannerStats& planner_stats,
               const ReaderStats& reader_stats, const S3Stats& s3_stats, const ExtStats& ext_stats,
               const iceberg::PositionalDeleteStats& positional_delete_stats,
               const iceberg::EqualityDeleteStats& equality_delete_stats, int segment_id);

void SendGandivaFilter(const std::string& gandiva_filter);
void SendPotentialRowGroupFilter(const std::string& filter_string);

}  // namespace tea::debug
