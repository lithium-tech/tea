#include "tea/observability/return_fl.h"

#include <sstream>

namespace tea {

namespace {

std::string AddFileLine(std::string_view file, int line, std::string_view str) {
  if (size_t last_slash = file.rfind('/'); last_slash != std::string_view::npos) {
    file.remove_prefix(last_slash + 1);
  }
  std::stringstream ss;
  ss << "[" << file << ":" << line << "] " << str;
  return ss.str();
}

}  // namespace

arrow::Status NotOK(std::string_view file, int line, const arrow::Status& status, const std::string& msg) {
  if (msg.size()) {
    auto new_msg = status.message() + " " + msg;
    return status.WithMessage(AddFileLine(file, line, new_msg));
  }
  return status.WithMessage(AddFileLine(file, line, status.message()));
}

}  // namespace tea
