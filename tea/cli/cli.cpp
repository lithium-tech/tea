#include <arrow/status.h>

#include <functional>
#include <iostream>
#include <optional>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "tea/cli/validate.h"

ABSL_FLAG(std::string, mode, "", "modes (validate-json-config, validate-profile-to-tables-mapping)");
ABSL_FLAG(bool, version, false, "print version if set");
ABSL_FLAG(std::string, json_config_path, "", "path to json-config");
ABSL_FLAG(std::string, json_schema_config_path, "", "path to json-schema-config");
ABSL_FLAG(std::string, profile_to_tables_mapping_path, "", "path to profile to tables mapping");
ABSL_FLAG(std::string, profile, "", "profile");

#if 0
arrow::Status PrintConfig(const std::string& config_path, const std::string& profile);
#endif

int Invoke(std::function<arrow::Status()> func) {
  try {
    auto status = func();
    if (!status.ok()) {
      std::cerr << status.ToString() << std::endl;
      return 1;
    }
    return 0;
  } catch (const arrow::Status& status) {
    std::cerr << status.ToString() << std::endl;
    return 1;
  } catch (...) {
    std::cerr << "Unknown error" << std::endl;
    return 1;
  }
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  if (absl::GetFlag(FLAGS_version)) {
    std::cout << TEA_VERSION << std::endl;
    return 0;
  }

  const std::string mode = absl::GetFlag(FLAGS_mode);
  if (mode.empty()) {
    std::cerr << "mode is not set" << std::endl;
    return 1;
  }

  if (mode == "validate-json-config") {
    const std::string config_json_path = absl::GetFlag(FLAGS_json_config_path);
    if (config_json_path.empty()) {
      std::cerr << "config_json_path is not set" << std::endl;
      return 1;
    }
    const std::string config_json_schema_path = absl::GetFlag(FLAGS_json_schema_config_path);
    const std::string profile = absl::GetFlag(FLAGS_profile);

    return Invoke([&]() {
      return tea::cli::ValidateJsonConfig(
          config_json_path, !config_json_schema_path.empty() ? std::optional(config_json_schema_path) : std::nullopt,
          profile);
    });
  }

  if (mode == "validate-profile-to-tables-mapping") {
    const std::string profile_to_tables_mapping_path = absl::GetFlag(FLAGS_profile_to_tables_mapping_path);
    if (profile_to_tables_mapping_path.empty()) {
      std::cerr << "profile_to_tables_mapping_path is not set" << std::endl;
      return 1;
    }
    return Invoke([&]() { return tea::cli::ValidateProfileToTablesMapping(profile_to_tables_mapping_path); });
  }

  std::cerr << "Unknown mode" << std::endl;
  return 1;
}
