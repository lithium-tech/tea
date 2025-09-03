#pragma once

#include <memory>
#include <utility>

#include "arrow/result.h"
#include "iceberg/tea_scan.h"

#include "tea/metadata/metadata.h"
#include "tea/observability/planner_stats.h"

namespace tea::meta {

class PlannedMeta {
 public:
  iceberg::ice_tea::ScanMetadata& GetDeletes() { return metadata_; }
  std::shared_ptr<AnnotatedDataEntryStream> GetStream() { return data_meta_stream_; }

  PlannedMeta(std::shared_ptr<AnnotatedDataEntryStream> data_meta_stream, iceberg::ice_tea::ScanMetadata metadata)
      : data_meta_stream_(data_meta_stream), metadata_(std::move(metadata)) {}

 private:
  std::shared_ptr<AnnotatedDataEntryStream> data_meta_stream_;
  // this metadata contains information about delete files only
  // TODO(gmusya): replace with ScanDeletesMetadata
  iceberg::ice_tea::ScanMetadata metadata_;
};

arrow::Result<std::pair<PlannedMeta, PlannerStats>> FromIcebergMetadata(iceberg::ice_tea::ScanMetadata&& meta);

}  // namespace tea::meta
