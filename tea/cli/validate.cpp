#include "tea/cli/validate.h"

#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "rapidjson/document.h"
#include "rapidjson/schema.h"
#include "rapidjson/stringbuffer.h"

#include "tea/common/config.h"

namespace tea::cli {

namespace {
inline std::string ReadFile(std::istream& is) {
  std::stringstream ss;
  ss << is.rdbuf();
  return ss.str();
}

// TODO(anyone): we don't yet have infra to deploy a schema file for profile-to-tables.json the way
// ci/deploy-tea-config.sh does for tea-config-schema.json, so the schema is hardcoded here for now.
// Keep this in sync with test/config/profile-to-tables-schema.json; switch to reading that file once deployable.
const char* const kProfileToTablesSchema = R"__({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "additionalProperties": false,
    "definitions": {
        "user-profile-override": {
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
        "user-profiles-to-username": {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "common_config": { "$ref": "#/definitions/user-profile-override" },
        "user-profiles": {
            "type": "object",
            "additionalProperties": { "$ref": "#/definitions/user-profile-override" }
        }
    }
})__";

arrow::Status ValidateAgainstJsonSchema(const rapidjson::Document& doc, const std::string& schema_content) {
  rapidjson::Document schema_doc;
  schema_doc.Parse(schema_content.data(), schema_content.size());
  if (schema_doc.HasParseError()) {
    return arrow::Status::ExecutionError("Schema parsing error: not a valid JSON");
  }

  rapidjson::SchemaDocument schema(schema_doc);
  rapidjson::SchemaValidator validator(schema);
  if (!doc.Accept(validator)) {
    rapidjson::StringBuffer document_pointer;
    validator.GetInvalidDocumentPointer().StringifyUriFragment(document_pointer);
    rapidjson::StringBuffer schema_pointer;
    validator.GetInvalidSchemaPointer().StringifyUriFragment(schema_pointer);
    return arrow::Status::ExecutionError("JSON schema validation error: document '", document_pointer.GetString(),
                                         "' violates schema keyword '",
                                         validator.GetInvalidSchemaKeyword() ? validator.GetInvalidSchemaKeyword() : "",
                                         "' at '", schema_pointer.GetString(), "'");
  }
  return arrow::Status::OK();
}

// Unlike tea::GetTableToProfileMapping/GetUsernameToProfileMapping (which silently drop an item claimed by
// multiple profiles, so Tea itself keeps ignoring the ambiguity at runtime), the validator treats this as a
// config mistake worth failing on: it's almost certainly not what the config author intended.
arrow::Status ValidateNoItemClaimedByMultipleProfiles(const rapidjson::Value& doc, const char* field_name) {
  if (!doc.HasMember(field_name) || !doc[field_name].IsObject()) {
    return arrow::Status::OK();
  }
  const auto& profile_to_items = doc[field_name];

  std::unordered_map<std::string, std::string> item_to_profile;
  for (auto iter = profile_to_items.MemberBegin(); iter != profile_to_items.MemberEnd(); ++iter) {
    if (!iter->name.IsString() || !iter->value.IsArray()) {
      continue;
    }
    const std::string profile = iter->name.GetString();
    for (const auto& elem : iter->value.GetArray()) {
      if (!elem.IsString()) {
        continue;
      }
      const std::string item = elem.GetString();
      auto [it, inserted] = item_to_profile.try_emplace(item, profile);
      if (!inserted && it->second != profile) {
        return arrow::Status::ExecutionError("Field '", field_name, "': '", item,
                                             "' is listed under multiple profiles ('", it->second, "' and '", profile,
                                             "')");
      }
    }
  }
  return arrow::Status::OK();
}
}  // namespace

arrow::Status ValidateJsonConfig(const std::string& config_path, const std::optional<std::string>& schema_path,
                                 const std::string& profile) {
  tea::Config config;
  ARROW_RETURN_NOT_OK(config.FromJsonFile(config_path, schema_path, profile));

  return arrow::Status::OK();
}

arrow::Status ValidateProfileToTablesMapping(const std::string& mapping_path) {
  std::ifstream input_config(mapping_path);
  if (!input_config.is_open()) {
    return arrow::Status::ExecutionError("Could not open file ", mapping_path, " for reading");
  }

  std::string s = ReadFile(input_config);
  ARROW_RETURN_NOT_OK(tea::GetTableToProfileMapping(s).status());
  ARROW_RETURN_NOT_OK(tea::GetUsernameToProfileMapping(s).status());
  tea::Config dummy_config;
  ARROW_RETURN_NOT_OK(tea::ApplyCommonConfigOverride(s, &dummy_config));

  rapidjson::Document doc;
  doc.Parse(s.data(), s.size());
  if (doc.HasParseError()) {
    return arrow::Status::ExecutionError("Profile-to-table parsing error: not a valid JSON");
  }

  ARROW_RETURN_NOT_OK(ValidateAgainstJsonSchema(doc, kProfileToTablesSchema));
  ARROW_RETURN_NOT_OK(ValidateNoItemClaimedByMultipleProfiles(doc, "profile-to-tables"));
  return ValidateNoItemClaimedByMultipleProfiles(doc, "user-profiles-to-username");
}

}  // namespace tea::cli
