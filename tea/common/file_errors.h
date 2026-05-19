#pragma once

#include <string>

#include "arrow/status.h"

namespace tea {

inline arrow::Status AnnotateFileError(const arrow::Status& status, const std::string& file_name) {
  if (file_name.empty()) {
    return status;
  }
  const std::string suffix = " (" + file_name + ")";
  const std::string message = status.message();
  if (message.ends_with(suffix)) {
    return status;
  }
  return status.WithMessage(message + suffix);
}

}  // namespace tea
