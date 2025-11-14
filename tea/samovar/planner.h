#pragma once

#include <iceberg/tea_scan.h>

#include <memory>
#include <string>
#include <utility>

#include "arrow/result.h"

#include "tea/common/config.h"
#include "tea/metadata/planner.h"
#include "tea/samovar/samovar_data_client.h"
#include "tea/util/cancel.h"

namespace tea::samovar {

enum class SamovarRole {
  kCoordinator,
  kFollower,
};

int GetCoordinator(const std::string& session_id, const TableSource& table_source, int segment_count);

std::shared_ptr<ISamovarDataClient> MakeSamovarDataClient(const SamovarConfig& config, const std::string& queue_name,
                                                          int segment_id, int segment_count, SamovarRole role,
                                                          const CancelToken& cancel_token);

arrow::Result<PlannerStats> FillSamovar(const Config& config, iceberg::ice_tea::ScanMetadata&& meta, int segment_count,
                                        std::shared_ptr<ISamovarDataClient> samovar_client);

arrow::Result<std::pair<meta::PlannedMeta, PlannerStats>> FromSamovar(
    const Config& config, int segment_id, const std::string& queue_name, bool is_metadata_already_written,
    std::shared_ptr<ISamovarDataClient> samovar_client);

}  // namespace tea::samovar
