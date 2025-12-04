#pragma once

#include <memory>

#include "iceberg/filter/stats_filter/stats_filter.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_entry_stats_getter.h"
#include "iceberg/schema.h"

namespace tea {

class FilteringEntriesStream : public iceberg::ice_tea::IcebergEntriesStream {
 public:
  FilteringEntriesStream(std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> input, iceberg::filter::NodePtr expr,
                         std::shared_ptr<iceberg::Schema> schema, int64_t timestamp_to_timestamptz_shift_us)
      : input_(input),
        filter_(expr, iceberg::filter::StatsFilter::Settings{.timestamp_to_timestamptz_shift_us =
                                                                 timestamp_to_timestamptz_shift_us}),
        schema_(schema) {}

  std::optional<iceberg::ManifestEntry> ReadNext() override {
    while (true) {
      auto maybe_entry = input_->ReadNext();
      if (!maybe_entry.has_value()) {
        return std::nullopt;
      }

      iceberg::ManifestEntryStatsGetter stats_getter(maybe_entry.value(), schema_);
      if (filter_.ApplyFilter(stats_getter) == iceberg::filter::MatchedRows::kNone) {
        continue;
      }

      return maybe_entry.value();
    }
  }

 private:
  std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> input_;
  iceberg::filter::StatsFilter filter_;
  std::shared_ptr<iceberg::Schema> schema_;
};

}  // namespace tea
