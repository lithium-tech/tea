#pragma once

#include <iconv.h>

#include "arrow/status.h"

#include "tea/table/bridge.h"

namespace tea {

class SharedState {
 public:
  arrow::Status InitializeConverter(int db_encoding);
  arrow::Status FinalizeConverter();
  CharsetConverter GetCharsetConverter(struct FmgrInfo* proc);

 private:
  iconv_t converter_ = nullptr;
};

SharedState* GetSharedState();
void InitializeSharedState();
void FinalizeSharedState();

}  // namespace tea
