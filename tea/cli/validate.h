#pragma once

#include <optional>
#include <string>

#include "arrow/status.h"

namespace tea::cli {

arrow::Status ValidateJsonConfig(const std::string& config_path, const std::optional<std::string>& schema_path,
                                 const std::string& profile);

arrow::Status ValidateProfileToTablesMapping(const std::string& mapping_path);

}  // namespace tea::cli
