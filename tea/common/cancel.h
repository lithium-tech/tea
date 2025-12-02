#pragma once

#include <memory>
#include <optional>
#include <string>

#include "iceberg/manifest_entry.h"

#include "tea/util/cancel.h"

namespace tea {

class CancellingStream : public iceberg::ice_tea::IcebergEntriesStream {
 public:
  CancellingStream(std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> stream, const CancelToken& cancel_token)
      : stream_(stream), cancel_token_(cancel_token) {}

  std::optional<iceberg::ManifestEntry> ReadNext() override {
    if (cancel_token_.IsCancelled()) {
      throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": query is cancelled");
    }
    return stream_->ReadNext();
  }

 private:
  std::shared_ptr<iceberg::ice_tea::IcebergEntriesStream> stream_;
  const CancelToken& cancel_token_;
};

}  // namespace tea
