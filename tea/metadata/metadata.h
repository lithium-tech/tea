#pragma once

#include <iceberg/schema.h>
#include <iceberg/streams/iceberg/data_entries_meta_stream.h>
#include <iceberg/tea_scan.h>

#include <memory>
#include <queue>
#include <utility>
#include <vector>

namespace tea {

struct ReaderStats;

namespace meta {

struct IMetadataScheduler {
  virtual std::vector<iceberg::AnnotatedDataPath> GetNextMetadata(size_t num_data_files) = 0;

  virtual void UpdateMetrics(ReaderStats& stats) {}

  virtual ~IMetadataScheduler() = default;
};

class AnnotatedDataEntryStream : public iceberg::IAnnotatedDataPathStream {
 public:
  explicit AnnotatedDataEntryStream(std::shared_ptr<IMetadataScheduler> meta) : meta_(meta) {}

  std::shared_ptr<iceberg::AnnotatedDataPath> ReadNext() override {
    if (entries_.empty()) {
      auto received_entries = meta_->GetNextMetadata(1);
      if (received_entries.empty()) {
        return nullptr;
      }
      for (auto& entry : received_entries) {
        entries_.emplace(std::move(entry));
      }
    }
    auto entry = entries_.front();
    entries_.pop();
    return std::make_shared<iceberg::AnnotatedDataPath>(std::move(entry));
  }

  std::shared_ptr<IMetadataScheduler> GetMeta() const { return meta_; }

 private:
  std::shared_ptr<IMetadataScheduler> meta_;
  std::queue<iceberg::AnnotatedDataPath> entries_;
};

}  // namespace meta

}  // namespace tea
