#pragma once

#include <iceberg/common/fs/filesystem_provider.h>
#include <iceberg/tea_scan.h>

#include <deque>
#include <memory>
#include <string>
#include <utility>

#include "arrow/result.h"

#include "tea/common/config.h"
#include "tea/metadata/planner.h"
#include "tea/samovar/single_queue_client.h"
#include "tea/util/cancel.h"

namespace tea::samovar {

int GetCoordinator(const std::string& session_id, const TableSource& table_source, int segment_count);

std::shared_ptr<SingleQueueClient> MakeSamovarDataClient(const SamovarConfig& config, const std::string& queue_name,
                                                         int segment_id, int segment_count, SamovarRole role,
                                                         const CancelToken& cancel_token);

arrow::Result<PlannerStats> FillSamovar(const Config& config, iceberg::ice_tea::ScanMetadata&& meta, int segment_count,
                                        std::shared_ptr<SingleQueueClient> samovar_client);

arrow::Result<PlannerStats> FillSamovarWithManifests(const Config& config, std::shared_ptr<iceberg::Schema> schema,
                                                     std::deque<iceberg::ManifestFile>, int segment_count,
                                                     std::shared_ptr<SingleQueueClient> samovar_client);

arrow::Result<std::pair<meta::PlannedMeta, PlannerStats>> FromSamovar(
    const Config& config, int segment_id, const std::string& queue_name,
    std::shared_ptr<SingleQueueClient> samovar_client, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider,
    const CancelToken& cancel_token, iceberg::filter::NodePtr filter_expr, int64_t timestamp_to_timestamptz_shift_us);

}  // namespace tea::samovar
