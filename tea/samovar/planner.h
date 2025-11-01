#pragma once

#include <iceberg/tea_scan.h>

#include <memory>
#include <string>
#include <utility>

#include "arrow/result.h"
#include "iceberg/common/fs/filesystem_provider.h"

#include "tea/common/config.h"
#include "tea/metadata/planner.h"
#include "tea/util/cancel.h"

namespace tea::samovar {

enum class SamovarRole {
  kCoordinator,
  kFollower,
};

int GetCoordinator(const std::string& session_id, const TableSource& table_source, int segment_count);

arrow::Result<PlannerStats> FillSamovar(const Config& config, iceberg::ice_tea::ScanMetadata&& meta, int segment_id,
                                        int segment_count, const std::string& queue_name,
                                        const std::string& compressor_name, const CancelToken& cancel_token);

arrow::Result<std::pair<meta::PlannedMeta, PlannerStats>> FromSamovar(const Config& config, int segment_id,
                                                                      int segment_count, const std::string& queue_name,
                                                                      const std::string& compressor_name,
                                                                      const CancelToken& cancel_token,
                                                                      bool is_metadata_already_written);

}  // namespace tea::samovar
