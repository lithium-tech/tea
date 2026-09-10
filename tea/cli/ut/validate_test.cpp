#include "tea/cli/validate.h"

#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/scoped_temp_dir.h"

namespace tea::cli {
namespace {

const char* const kSchemaContent = R"__({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "additionalProperties": false,
    "definitions": {
        "profile_override": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "limits": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "max_total_s3_bytes_read": { "type": "integer", "minimum": 0 }
                    }
                }
            }
        }
    },
    "properties": {
        "profile-to-tables": {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "profile-to-username": {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "common_config": { "$ref": "#/definitions/profile_override" },
        "profiles": {
            "type": "object",
            "additionalProperties": { "$ref": "#/definitions/profile_override" }
        }
    }
})__";

const char* const kValidMapping = R"__({
    "profile-to-tables": {
        "table_profile": ["some.table"]
    },
    "profile-to-username": {
        "someprofile1": ["name1"]
    },
    "common_config": {
        "limits": {
            "max_total_s3_bytes_read": 1000000000
        }
    },
    "profiles": {
        "someprofile1": {
            "limits": {
                "max_total_s3_bytes_read": 10000
            }
        }
    }
})__";

const char* const kValidJsonConfig = R"__({
    "common": {
        "s3": {
            "access_key": "EXAMPLE",
            "secret_key": "EXAMPLE",
            "endpoint_override": "127.0.0.1:9000",
            "scheme": "http",
            "connect_timeout_ms": 1000,
            "request_timeout_ms": 3000
        }
    }
})__";

void WriteFile(const std::filesystem::path& path, const std::string& content) {
  std::ofstream f(path);
  f << content;
}

struct ValidateTest : public testing::Test {
  iceberg::ScopedTempDir dir_;
};

TEST_F(ValidateTest, JsonConfigFileNotFound) {
  EXPECT_FALSE(ValidateJsonConfig((dir_.path() / "no-such-file.json").string(), std::nullopt, "").ok());
}

TEST_F(ValidateTest, JsonConfigValid) {
  auto path = dir_.path() / "tea-config.json";
  WriteFile(path, kValidJsonConfig);

  ASSERT_OK(ValidateJsonConfig(path.string(), std::nullopt, ""));
}

TEST_F(ValidateTest, MappingFileNotFound) {
  EXPECT_FALSE(ValidateProfileToTablesMapping((dir_.path() / "no-such-file.json").string(), std::nullopt).ok());
}

TEST_F(ValidateTest, MappingValidWithoutSchema) {
  auto path = dir_.path() / "profile-to-tables.json";
  WriteFile(path, kValidMapping);

  ASSERT_OK(ValidateProfileToTablesMapping(path.string(), std::nullopt));
}

TEST_F(ValidateTest, MappingMissingProfileToTablesIsNotAnError) {
  auto path = dir_.path() / "profile-to-tables.json";
  WriteFile(path, R"__({ "a": "b" })__");

  ASSERT_OK(ValidateProfileToTablesMapping(path.string(), std::nullopt));
}

TEST_F(ValidateTest, MappingMalformedProfileToTablesIsAnError) {
  auto path = dir_.path() / "profile-to-tables.json";
  WriteFile(path, R"__({ "profile-to-tables": "not-an-object" })__");

  EXPECT_FALSE(ValidateProfileToTablesMapping(path.string(), std::nullopt).ok());
}

TEST_F(ValidateTest, MappingValidWithSchema) {
  auto mapping_path = dir_.path() / "profile-to-tables.json";
  WriteFile(mapping_path, kValidMapping);
  auto schema_path = dir_.path() / "profile-to-tables-schema.json";
  WriteFile(schema_path, kSchemaContent);

  ASSERT_OK(ValidateProfileToTablesMapping(mapping_path.string(), schema_path.string()));
}

TEST_F(ValidateTest, SchemaRejectsExtraTopLevelField) {
  auto mapping_path = dir_.path() / "profile-to-tables.json";
  WriteFile(mapping_path, R"__({
    "profile-to-tables": { "table_profile": ["some.table"] },
    "unexpected_field": true
})__");
  auto schema_path = dir_.path() / "profile-to-tables-schema.json";
  WriteFile(schema_path, kSchemaContent);

  EXPECT_FALSE(ValidateProfileToTablesMapping(mapping_path.string(), schema_path.string()).ok());
}

TEST_F(ValidateTest, SchemaRejectsDisallowedOverrideField) {
  auto mapping_path = dir_.path() / "profile-to-tables.json";
  WriteFile(mapping_path, R"__({
    "profile-to-tables": { "table_profile": ["some.table"] },
    "common_config": {
        "s3": { "access_key": "not-allowed" }
    }
})__");
  auto schema_path = dir_.path() / "profile-to-tables-schema.json";
  WriteFile(schema_path, kSchemaContent);

  EXPECT_FALSE(ValidateProfileToTablesMapping(mapping_path.string(), schema_path.string()).ok());
}

TEST_F(ValidateTest, SchemaFileNotFound) {
  auto mapping_path = dir_.path() / "profile-to-tables.json";
  WriteFile(mapping_path, kValidMapping);

  EXPECT_FALSE(
      ValidateProfileToTablesMapping(mapping_path.string(), (dir_.path() / "no-such-schema.json").string()).ok());
}

}  // namespace
}  // namespace tea::cli
