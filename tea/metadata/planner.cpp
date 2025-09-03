#include "tea/metadata/planner.h"

#include <iceberg/streams/iceberg/data_entries_meta_stream.h>
#include <iceberg/streams/iceberg/plan.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "iceberg/tea_scan.h"

#include "tea/metadata/metadata.h"
#include "tea/observability/planner_stats.h"

namespace tea::meta {

namespace {
class PlannedMetadata final : public IMetadataScheduler {
 public:
  explicit PlannedMetadata(std::vector<iceberg::AnnotatedDataPath> all_data_entries)
      : all_data_entries_(std::move(all_data_entries)) {
    std::sort(all_data_entries_.begin(), all_data_entries_.end(),
              [](const iceberg::AnnotatedDataPath& lhs, const iceberg::AnnotatedDataPath& rhs) {
                if (lhs.GetPartition() != rhs.GetPartition()) {
                  return lhs.GetPartition() < rhs.GetPartition();
                }
                return lhs.GetPath() < rhs.GetPath();
              });
  }

  std::vector<iceberg::AnnotatedDataPath> GetNextMetadata(size_t num_data_files) override {
    int files_to_fetch = std::min(num_data_files, all_data_entries_.size() - current_iterator_);
    if (files_to_fetch == 0) {
      is_end_ = true;
      return {};
    }

    auto result = std::vector<iceberg::AnnotatedDataPath>(
        all_data_entries_.begin() + current_iterator_, all_data_entries_.begin() + current_iterator_ + files_to_fetch);
    current_iterator_ += files_to_fetch;
    return result;
  }

 private:
  std::vector<iceberg::AnnotatedDataPath> all_data_entries_;
  size_t current_iterator_ = 0;
  bool is_end_ = false;
};

std::vector<iceberg::AnnotatedDataPath> ExtractDataEntries(iceberg::ice_tea::ScanMetadata& meta) {
  std::vector<iceberg::AnnotatedDataPath> all_data_entries;
  for (size_t partition_id = 0; partition_id < meta.partitions.size(); ++partition_id) {
    for (size_t layer_id_p1 = meta.partitions[partition_id].size(); layer_id_p1 >= 1; layer_id_p1--) {
      const auto layer_id = layer_id_p1 - 1;

      auto entries = std::move(meta.partitions[partition_id][layer_id].data_entries_);

      std::sort(entries.begin(), entries.end(), [&](const auto& lhs, const auto& rhs) { return lhs.path < rhs.path; });
      for (const auto& data_entry : entries) {
        std::vector<iceberg::AnnotatedDataPath::Segment> segments;
        for (const auto& part : data_entry.parts) {
          segments.emplace_back(iceberg::AnnotatedDataPath::Segment{.offset = part.offset, .length = part.length});
        }
        iceberg::AnnotatedDataPath result_annotated_path(
            iceberg::PartitionLayerFile(
                iceberg::PartitionLayer(static_cast<int>(partition_id), static_cast<int>(layer_id)), data_entry.path),
            std::move(segments));
        all_data_entries.emplace_back(std::move(result_annotated_path));
      }
    }
  }

  return all_data_entries;
}

}  // namespace

arrow::Result<std::pair<PlannedMeta, PlannerStats>> FromIcebergMetadata(iceberg::ice_tea::ScanMetadata&& meta) {
  // note that ExtractDataEntries modifies meta
  // TODO(gmusya): do something with it
  std::shared_ptr<PlannedMetadata> planned_meta_stream = std::make_shared<PlannedMetadata>(ExtractDataEntries(meta));

  PlannedMeta result(std::make_shared<AnnotatedDataEntryStream>(planned_meta_stream), std::move(meta));
  return std::make_pair(std::move(result), PlannerStats{});
}

}  // namespace tea::meta
