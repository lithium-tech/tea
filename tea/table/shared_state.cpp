#include "tea/table/shared_state.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
#include "tea/common/iceberg_json.h"
#include "tea/common/utils.h"
#include "tea/observability/tea_log.h"
#include "teapot/teapot.pb.h"

namespace tea {

arrow::Status SharedState::InitializeConverter(int db_encoding) {
  if (!ConfigSource::GetConfig().features.substitute_illegal_code_points) {
    return arrow::Status::OK();
  }
  auto enc = InitializeIconv(db_encoding);
  if (!enc.ok()) {
    return enc.status();
  }
  converter_ = *enc;
  return arrow::Status::OK();
}

arrow::Status SharedState::FinalizeConverter() {
  if (!converter_) {
    return arrow::Status::OK();
  }
  if (auto status = FinalizeIconv(converter_); !status.ok()) {
    return status;
  }
  converter_ = nullptr;
  return arrow::Status::OK();
}

CharsetConverter SharedState::GetCharsetConverter(struct FmgrInfo* proc) {
  if (converter_) return MakeIconvConverter(converter_);
  if (proc) return MakePgConverter(proc);
  return MakeIdentityConverter();
}

static std::unique_ptr<SharedState> shared_state;

SharedState* GetSharedState() { return shared_state.get(); }

void InitializeSharedState() {
  if (shared_state) {
    throw arrow::Status::ExecutionError("Shared state is already initialized");
  }
  shared_state = std::make_unique<SharedState>();
}

void FinalizeSharedState() {
  if (!shared_state) {
    throw arrow::Status::ExecutionError("Shared state is already finalized");
  }
  (void)shared_state->FinalizeConverter();
  shared_state.reset();
}

}  // namespace tea
