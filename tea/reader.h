#pragma once

#include <iceberg/common/logger.h>
#include <iceberg/schema.h>
#include <iceberg/streams/arrow/stream.h>
#include <iceberg/streams/iceberg/data_entries_meta_stream.h>
#include <iceberg/streams/iceberg/iceberg_batch.h>

#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/string.h"
#include "iceberg/common/batch.h"
#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/common/fs/s3.h"
#include "iceberg/filter/row_filter/row_filter.h"
#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
#include "tea/metadata/metadata.h"
#include "tea/metadata/planner.h"
#include "tea/observability/reader_stats.h"
#include "tea/observability/s3_stats.h"
#include "tea/table/bridge.h"
#include "tea/util/logger.h"
#include "tea/util/multishot_timer.h"

namespace tea {

class Reader {
 public:
  struct SerializedFilter {
    std::string extracted;
    std::string row;
  };

  explicit Reader(const Config &config, std::shared_ptr<iceberg::IFileSystemProvider> fs_provider);
  ~Reader();

  static arrow::Status Finalize();

  static arrow::Status Initialize(int db_encoding);

 public:
  arrow::Result<std::optional<iceberg::BatchWithSelectionVector>> GetNextBatch();

  arrow::Status Plan(meta::PlannedMeta meta, const Reader::SerializedFilter &filter, bool postfilter_on_gp);

  template <typename It>
  void SetColumns(It first, It last) {
    // TODO(gmusya): check that there are no different columns with same lowercase name
    columns_.clear();
    for (auto it = first; it != last; ++it) {
      columns_.push_back(
          ReaderColumn{arrow::internal::AsciiToLower(it->name), it->index, it->type, it->type_mode, it->remote_only});
    }
  }

 public:
  std::tuple<ReaderStats, iceberg::PositionalDeleteStats, iceberg::EqualityDeleteStats, S3Stats> GetStats() const;

  const std::vector<ReaderColumn> &columns() const { return columns_; }

  struct FilesystemStats {
    S3Stats s3_stats;
    std::shared_ptr<OneThreadMultishotTimer> wait_read_stats;
    mutable std::mutex s3_read_stats_lock_;
  };

 private:
  std::shared_ptr<Logger> InitializeLogger();

  void LogPotentialStatsFilter();

  using DataEntry = iceberg::ice_tea::DataEntry;

 private:
  std::string current_file_name_ = "";

  std::shared_ptr<FilesystemStats> fs_stats_;
  Config config_;

  mutable ReaderStats stats_ = {};

  std::vector<ReaderColumn> columns_;

  mutable std::shared_ptr<iceberg::CountingS3RetryStrategy> retry_strategy_;

  OneThreadMultishotTimer positional_delete_timer_;
  OneThreadMultishotTimer equality_delete_timer_;
  OneThreadMultishotTimer filter_build_timer_;
  OneThreadMultishotTimer filter_apply_timer_;

  std::shared_ptr<meta::AnnotatedDataEntryStream> entries_stream_;
  std::shared_ptr<iceberg::Schema> schema_;

  mutable bool at_least_one_matching_row_in_file_ = true;
  mutable bool potential_row_group_filter_logged_ = false;

  iceberg::PositionalDeleteStats positional_delete_stats_;
  iceberg::EqualityDeleteStats equality_delete_stats_;

  std::shared_ptr<iceberg::filter::RowFilter> row_filter_;

  iceberg::IcebergStreamPtr stream_;
  std::shared_ptr<iceberg::IFileSystemProvider> fs_provider_;
};

}  // namespace tea
