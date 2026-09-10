#include "tea/cli/validate.h"

#include <fstream>
#include <sstream>

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
}  // namespace

arrow::Status ValidateJsonConfig(const std::string& config_path, const std::optional<std::string>& schema_path,
                                 const std::string& profile) {
  tea::Config config;
  ARROW_RETURN_NOT_OK(config.FromJsonFile(config_path, schema_path, profile));

  return arrow::Status::OK();
}

arrow::Status ValidateProfileToTablesMapping(const std::string& mapping_path,
                                             const std::optional<std::string>& schema_path) {
  std::ifstream input_config(mapping_path);
  if (!input_config.is_open()) {
    return arrow::Status::ExecutionError("Could not open file ", mapping_path, " for reading");
  }

  std::string s = ReadFile(input_config);
  ARROW_RETURN_NOT_OK(tea::GetTableToProfileMapping(s).status());
  ARROW_RETURN_NOT_OK(tea::GetUsernameToProfileMapping(s).status());
  tea::Config dummy_config;
  ARROW_RETURN_NOT_OK(tea::ApplyCommonConfigOverride(s, &dummy_config));

  if (schema_path.has_value()) {
    std::ifstream input_schema(*schema_path);
    if (!input_schema.is_open()) {
      return arrow::Status::ExecutionError("Could not open file ", *schema_path, " for reading");
    }
    std::string schema_content = ReadFile(input_schema);

    rapidjson::Document doc;
    doc.Parse(s.data(), s.size());
    if (doc.HasParseError()) {
      return arrow::Status::ExecutionError("Profile-to-table parsing error: not a valid JSON");
    }

    ARROW_RETURN_NOT_OK(ValidateAgainstJsonSchema(doc, schema_content));
  }

  return arrow::Status::OK();
}

}  // namespace tea::cli
