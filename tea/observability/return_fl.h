#pragma once

#include <string>
#include <string_view>

#include "arrow/result.h"
#include "arrow/status.h"

namespace tea {

#define RETURN_FL_NOT_OK_MSG(expr, msg)              \
  do {                                               \
    auto&& status = (expr);                          \
    if (ARROW_PREDICT_FALSE(!status.ok())) {         \
      return NotOK(__FILE__, __LINE__, status, msg); \
    }                                                \
  } while (false)

#define RETURN_FL_NOT_OK(status) RETURN_FL_NOT_OK_MSG(status, {})

arrow::Status NotOK(std::string_view file, int line, const arrow::Status& status, const std::string& msg = {});

template <typename T>
arrow::Result<T> NotOK(std::string_view file, int line, const arrow::Result<T>& result, const std::string& msg = {}) {
  return NotOK(file, line, result.status(), msg);
}

}  // namespace tea
