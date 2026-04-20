#pragma once

#include <stdint.h>

#include <string>
#include <string_view>

#include "iceberg/equality_delete/stats.h"
#include "iceberg/positional_delete/stats.h"

#include "tea/observability/ext_stats.h"
#include "tea/observability/planner_stats.h"
#include "tea/observability/s3_stats.h"
#include "tea/util/measure.h"

namespace tea {

// after updating stats update test/check_log_events.py
struct ReaderStats {
  int64_t data_files_read = 0;
  int64_t row_groups_read = 0;
  int64_t row_groups_skipped_filter = 0;
  int64_t rows_read = 0;
  int64_t rows_skipped_positional_delete = 0;
  int64_t rows_skipped_equality_delete = 0;
  int64_t rows_skipped_filter = 0;
  int64_t rows_skipped_prefilter = 0;
  // TODO(gmusya): for some reason PositionalDeleteStats defined in iceberg-cxx. Fix
  int64_t positional_delete_rows_ignored = 0;
  int64_t columns_read = 0;
  int64_t columns_for_greenplum = 0;
  int64_t columns_equality_delete = 0;
  int64_t columns_only_for_equality_delete = 0;
  DurationTicks positional_delete_apply_duration = 0;
  DurationTicks equality_delete_apply_duration = 0;
  DurationTicks gandiva_filter_build_duration = 0;
  DurationTicks gandiva_filter_apply_duration = 0;
  DurationTicks fetch_duration = 0;
  DurationTicks read_duration = 0;
};

std::string FormatStats(std::string_view event_type, std::string_view session_id, uint64_t scan_identifier,
                        std::string_view version, DurationTicks duration_ticks, double ticks_per_second,
                        const PlannerStats&, const ReaderStats& reader_stats, const S3Stats& s3_stats,
                        const ExtStats& ext_stats, const iceberg::PositionalDeleteStats& positional_delete_stats,
                        const iceberg::EqualityDeleteStats& equality_delete_stats, DurationTicks prefetch_duration,
                        DurationTicks wait_duration);
}  // namespace tea
