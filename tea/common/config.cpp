#include "tea/common/config.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/status.h"
#include "iceberg/common/fs/url.h"
#include "iceberg/tea_scan.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/schema.h"

#include "tea/observability/tea_log.h"

namespace tea {

namespace {

constexpr std::string_view kTeaSchema = "tea://";

void LoadEnvDefaults(Config* config) {
  if (auto* var = getenv("AWS_ENDPOINT_URL")) {
    config->s3.endpoint_override = std::string(var);
  }
  if (auto* var = getenv("AWS_DEFAULT_REGION")) {
    config->s3.region = std::string(var);
  }
  if (auto* var = getenv("AWS_ACCESS_KEY_ID")) {
    config->s3.access_key = std::string(var);
  }
  if (auto* var = getenv("AWS_SECRET_ACCESS_KEY")) {
    config->s3.secret_key = std::string(var);
  }
}

const rapidjson::Value* Advance(const rapidjson::Value* doc, const std::vector<std::string>& path) {
  const rapidjson::Value* result = doc;
  for (const auto& field_name : path) {
    if (!result->IsObject()) {
      return nullptr;
    }
    if (!result->HasMember(field_name.data())) {
      return nullptr;
    }
    result = &(*result)[field_name.data()];
  }
  return result;
}

// Need similar interface for code reuse
bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         std::string* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsString()) {
    return false;
  }
  *out = value->GetString();
  return true;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         std::vector<int>* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  out->clear();
  if (!value->IsArray()) {
    return false;
  }
  for (const auto& elem : value->GetArray()) {
    if (!elem.IsInt()) {
      return false;
    }
    out->emplace_back(elem.GetInt());
  }
  return true;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         std::vector<std::string>* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  out->clear();
  if (!value->IsArray()) {
    return false;
  }
  for (const auto& elem : value->GetArray()) {
    if (!elem.IsString()) {
      return false;
    }
    out->emplace_back(elem.GetString());
  }
  return true;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         std::chrono::milliseconds* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsInt64()) {
    return false;
  }
  *out = std::chrono::milliseconds(value->GetInt64());
  return true;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         std::chrono::seconds* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsInt64()) {
    return false;
  }
  *out = std::chrono::seconds(value->GetInt64());
  return true;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         int* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsInt()) {
    return false;
  }
  *out = value->GetInt();
  return true;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         double* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsDouble()) {
    return false;
  }
  *out = value->GetDouble();
  return true;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         uint64_t* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsUint64()) {
    return false;
  }
  *out = value->GetUint64();
  return true;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         bool* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsBool()) {
    return false;
  }
  *out = value->GetBool();
  return true;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         BackoffType* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsString()) {
    return false;
  }
  std::string str = value->GetString();
  std::transform(str.begin(), str.end(), str.begin(), tolower);
  if (str == "no") {
    *out = BackoffType::kNoBackoff;
    return true;
  } else if (str == "lin") {
    *out = BackoffType::kLinearBackoff;
    return true;
  } else if (str.empty() || str == "exp") {
    *out = BackoffType::kExponentialBackoff;
    return true;
  }
  return false;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         SplitType* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsString()) {
    return false;
  }
  std::string str = value->GetString();
  std::transform(str.begin(), str.end(), str.begin(), tolower);
  if (str == "whole") {
    *out = SplitType::kWholeDataEntry;
    return true;
  } else if (str == "offsets") {
    *out = SplitType::kOffsets;
    return true;
  }
  return false;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         CatalogConfig::CatalogType* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsString()) {
    return false;
  }
  std::string str = value->GetString();
  std::transform(str.begin(), str.end(), str.begin(), tolower);
  if (str.empty() || str == "hms") {
    *out = CatalogConfig::CatalogType::kHMS;
    return true;
  } else if (str == "rest") {
    *out = CatalogConfig::CatalogType::kREST;
    return true;
  }
  return false;
}

bool Get(const rapidjson::Value* doc, std::string_view section_prefix, std::string_view section, const std::string& key,
         BalancerType* out) {
  const rapidjson::Value* value = Advance(doc, {std::string(section), key});
  if (!value) {
    return false;
  }
  if (!value->IsString()) {
    return false;
  }
  std::string str = value->GetString();
  std::transform(str.begin(), str.end(), str.begin(), tolower);
  if (str.empty() || str == "one_queue") {
    *out = BalancerType::kOneQueue;
    return true;
  }
  return false;
}

template <typename Source, typename T>
bool GetOptional(Source* source, std::string_view section_prefix, std::string_view section, const std::string& key,
                 std::optional<T>* out) {
  T result;
  if (Get(source, section_prefix, section, key, &result)) {
    *out = std::move(result);
    return true;
  } else {
    return false;
  }
}

bool GetEndpointsArray(const rapidjson::Value* source, std::string_view section_prefix, std::string_view section,
                       const std::string& key, std::vector<std::string>* out) {
  // try to read ["a:1", "b:2", "c:3"]
  if (Get(source, section_prefix, section, key, out)) {
    return true;
  }

  // try to read "a:1,b:2,c:3"
  std::string arr_as_str;
  if (!Get(source, section_prefix, section, key, &arr_as_str)) {
    return false;
  }
  out->clear();
  *out = absl::StrSplit(arr_as_str, ",");
  return true;
}

template <typename Source>
bool GetEndpoints(Source* source, std::string_view section_prefix, std::string_view section, const std::string& key,
                  std::vector<Endpoint>* endpoints) {
  std::vector<std::string> endpoints_as_str;
  if (!GetEndpointsArray(source, section_prefix, section, key, &endpoints_as_str)) {
    return false;
  }
  endpoints->clear();
  auto log_typo = [&](const std::string_view& endpoint) {
    TEA_LOG(std::string("Typo in ") + absl::StrCat(section_prefix, section, ".", key) + ": \"" + std::string(endpoint) +
            "\"");
  };

  for (const auto& endpoint_str : endpoints_as_str) {
    std::vector<std::string> splitted_endpoint = absl::StrSplit(endpoint_str, ":");
    if (splitted_endpoint.size() != 2) {
      log_typo(endpoint_str);
      continue;
    }
    int port;
    if (!absl::SimpleAtoi(splitted_endpoint[1], &port)) {
      log_typo(endpoint_str);
      continue;
    }
    if (!(0 <= port && port <= std::numeric_limits<uint16_t>::max())) {
      log_typo(endpoint_str);
      continue;
    }
    endpoints->emplace_back(Endpoint{.host = splitted_endpoint[0], .port = static_cast<uint16_t>(port)});
  }
  return true;
}

template <typename Source>
void GetBackoffInfo(Source* src, BackoffInfo* config, std::string_view section_prefix,
                    std::string_view keys_prefix = "") {
  Get(src, section_prefix, "samovar", absl::StrCat(keys_prefix, "backoff_type"), &config->backoff_type);
  // Sleep time in milliseconds
  Get(src, section_prefix, "samovar", absl::StrCat(keys_prefix, "linear_backoff_time_to_sleep_ms"),
      &config->linear_backoff_time_to_sleep_ms);
  // Increasing coefficient in exponential backoff
  // Compat: will be removed in future versions
  GetOptional(src, section_prefix, "samovar", absl::StrCat(keys_prefix, "exponentail_backoff_sleep_coef"),
              &config->exponential_backoff_sleep_coef);

  GetOptional(src, section_prefix, "samovar", absl::StrCat(keys_prefix, "exponential_backoff_sleep_coef"),
              &config->exponential_backoff_sleep_coef);

  // Upper bound on waiting time for exponential backoff
  // Compat: will be removed in future versions
  GetOptional(src, section_prefix, "samovar", absl::StrCat(keys_prefix, "exponentail_backoff_limit"),
              &config->exponential_backoff_limit);

  GetOptional(src, section_prefix, "samovar", absl::StrCat(keys_prefix, "exponential_backoff_limit"),  // deprecated
              &config->exponential_backoff_limit);
  GetOptional(src, section_prefix, "samovar", absl::StrCat(keys_prefix, "exponential_backoff_limit_ms"),
              &config->exponential_backoff_limit);
  Get(src, section_prefix, "samovar", absl::StrCat(keys_prefix, "limit_retries"), &config->limit_retries);
}

template <typename Source>
arrow::Status ReadValues(Source* src, Config* config, std::string_view section_prefix) {
  Get(src, section_prefix, "s3", "access_key", &config->s3.access_key);
  Get(src, section_prefix, "s3", "secret_key", &config->s3.secret_key);
  Get(src, section_prefix, "s3", "endpoint_override", &config->s3.endpoint_override);
  Get(src, section_prefix, "s3", "region", &config->s3.region);
  Get(src, section_prefix, "s3", "scheme", &config->s3.scheme);
  Get(src, section_prefix, "s3", "connect_timeout_ms", &config->s3.connect_timeout);
  Get(src, section_prefix, "s3", "request_timeout_ms", &config->s3.request_timeout);
  Get(src, section_prefix, "s3", "retry_max_attempts", &config->s3.retry_max_attempts);

  Get(src, section_prefix, "catalog", "type", &config->catalog.type);
  GetEndpoints(src, section_prefix, "catalog", "hms", &config->catalog.hms_endpoints);
  GetEndpoints(src, section_prefix, "catalog", "rest", &config->catalog.rest_endpoints);

  GetEndpoints(src, section_prefix, "hms", "hms", &config->hms_catalog.hms_endpoints);

  Get(src, section_prefix, "teapot", "location", &config->teapot.location);
  Get(src, section_prefix, "teapot", "timeout_ms", &config->teapot.timeout);

  Get(src, section_prefix, "limits", "max_cpu_threads", &config->limits.max_cpu_threads);
  Get(src, section_prefix, "limits", "max_io_threads", &config->limits.max_io_threads);
  Get(src, section_prefix, "limits", "parquet_buffer_size", &config->limits.parquet_buffer_size);
  Get(src, section_prefix, "limits", "arrow_buffer_rows", &config->limits.arrow_buffer_rows);
  Get(src, section_prefix, "limits", "equality_delete_max_rows", &config->limits.equality_delete_max_rows);
  Get(src, section_prefix, "limits", "equality_delete_max_mb_size", &config->limits.equality_delete_max_mb_size);
  Get(src, section_prefix, "limits", "metadata_cache_size", &config->limits.metadata_cache_size);
  Get(src, section_prefix, "limits", "grpc_max_message_size", &config->limits.grpc_max_message_size);
  Get(src, section_prefix, "limits", "json_max_message_size_on_master",
      &config->limits.json_max_message_size_on_master);
  Get(src, section_prefix, "limits", "samovar_distributed_metadata_parsing_files_threshold",
      &config->limits.samovar_distributed_metadata_parsing_files_threshold);
  Get(src, section_prefix, "limits", "samovar_max_total_data_files", &config->limits.samovar_max_total_data_files);
  Get(src, section_prefix, "limits", "samovar_max_total_positional_delete_files",
      &config->limits.samovar_max_total_positional_delete_files);

  Get(src, section_prefix, "experimental_features", "prefetch", &config->features.prefetch);
  Get(src, section_prefix, "experimental_features", "read_in_multiple_threads",
      &config->features.read_in_multiple_threads);
  Get(src, section_prefix, "experimental_features", "use_helper_thread", &config->features.use_helper_thread);
  Get(src, section_prefix, "experimental_features", "filter_ignored_op_exprs",
      &config->features.filter_ignored_op_exprs);
  Get(src, section_prefix, "experimental_features", "filter_ignored_func_exprs",
      &config->features.filter_ignored_func_exprs);

  Get(src, section_prefix, "features", "read_in_multiple_threads", &config->features.read_in_multiple_threads);
  Get(src, section_prefix, "features", "enable_row_group_filters", &config->features.enable_row_group_filters);
  Get(src, section_prefix, "features", "use_custom_heap_form_tuple", &config->features.use_custom_heap_form_tuple);
  Get(src, section_prefix, "features", "ext_table_filter_walker_for_projection",
      &config->features.ext_table_filter_walker_for_projection);
  Get(src, section_prefix, "features", "use_virtual_tuple", &config->features.use_virtual_tuple);
  Get(src, section_prefix, "features", "use_specialized_deletes", &config->features.use_specialized_deletes);
  Get(src, section_prefix, "features", "substitute_illegal_code_points",
      &config->features.substitute_illegal_code_points);
  Get(src, section_prefix, "features", "postfilter_on_gp", &config->features.postfilter_on_gp);
  Get(src, section_prefix, "features", "throw_if_memory_limit_exceeded",
      &config->features.throw_if_memory_limit_exceeded);
  Get(src, section_prefix, "features", "use_helper_thread", &config->features.use_helper_thread);
  Get(src, section_prefix, "features", "use_iceberg_metadata_partition_pruning",
      &config->features.use_iceberg_metadata_partition_pruning);
  Get(src, section_prefix, "features", "optimize_deletes_in_teapot_response",
      &config->features.optimize_deletes_in_teapot_response);
  Get(src, section_prefix, "features", "use_avro_projection_minimum_columns",
      &config->features.use_avro_projection_minimum_columns);

  Get(src, section_prefix, "json", "substitute_unicode_if_json_type", &config->json.substitute_unicode_if_json_type);

  Get(src, section_prefix, "debug", "test_gandiva_filter", &config->debug.test_gandiva_filter);
  Get(src, section_prefix, "debug", "test_stats", &config->debug.test_stats);
  Get(src, section_prefix, "debug", "use_slow_filesystem", &config->debug.use_slow_filesystem);
  Get(src, section_prefix, "debug", "slow_filesystem_delay_seconds", &config->debug.slow_filesystem_delay_seconds);
  Get(src, section_prefix, "debug", "intermediate_logs_period_seconds",
      &config->debug.intermediate_logs_period_seconds);

  Get(src, section_prefix, "metadata_access", "default_schema", &config->meta_access.default_schema);

  Get(src, section_prefix, "samovar", "use_samovar", &config->samovar_config.turn_on_samovar);

  // SyncBackoff = MetadataBackoff by default, but params can be overrided
  GetBackoffInfo(src, &config->samovar_config.metadata_backoff, section_prefix, "");
  config->samovar_config.sync_backoff = config->samovar_config.metadata_backoff;
  GetBackoffInfo(src, &config->samovar_config.sync_backoff, section_prefix, "sync_");

  Get(src, section_prefix, "samovar", "balancer_type", &config->samovar_config.balancer_type);
  Get(src, section_prefix, "samovar", "cluster_id", &config->samovar_config.cluster_id);
  // Maybe better to use different keys for redis and postgres
  GetEndpoints(src, section_prefix, "samovar", "samovar", &config->samovar_config.endpoints);

  Get(src, section_prefix, "samovar", "ttl_seconds", &config->samovar_config.ttl_seconds);
  Get(src, section_prefix, "samovar", "request_timeout", &config->samovar_config.request_timeout);  // deprecated
  Get(src, section_prefix, "samovar", "request_timeout_ms", &config->samovar_config.request_timeout);
  Get(src, section_prefix, "samovar", "connection_timeout", &config->samovar_config.connection_timeout);  // deprecated
  Get(src, section_prefix, "samovar", "connection_timeout_ms", &config->samovar_config.connection_timeout);

  GetOptional(src, section_prefix, "samovar", "first_request_fragments",
              &config->samovar_config.first_request_fragments);
  Get(src, section_prefix, "samovar", "split_type", &config->samovar_config.split_type);

  Get(src, section_prefix, "samovar", "batch_size", &config->samovar_config.batch_size);
  Get(src, section_prefix, "samovar", "compressor_name", &config->samovar_config.compressor_name);

  Get(src, section_prefix, "samovar", "wait_before_processing", &config->samovar_config.wait_before_processing);
  Get(src, section_prefix, "samovar", "min_time_before_processing_ms",
      &config->samovar_config.min_time_before_processing_ms);
  Get(src, section_prefix, "samovar", "max_time_before_processing_ms",
      &config->samovar_config.max_time_before_processing_ms);

  Get(src, section_prefix, "samovar", "need_sync_on_init", &config->samovar_config.need_sync_on_init);
  Get(src, section_prefix, "samovar", "allow_static_balancing", &config->samovar_config.allow_static_balancing);

  Get(src, section_prefix, "samovar", "first_slice_to_sleep", &config->samovar_config.first_slice_to_sleep);
  Get(src, section_prefix, "samovar", "sleep_per_slice_ms", &config->samovar_config.sleep_per_slice_ms);
  Get(src, section_prefix, "samovar", "max_sleep_time_ms", &config->samovar_config.max_sleep_time_ms);

  return arrow::Status::OK();
}

arrow::Status Load(const rapidjson::Document& document, std::string_view profile, Config* config) {
  LoadEnvDefaults(config);
  if (!document.IsObject()) {
    return arrow::Status::ExecutionError("Invalid tea-config.json flie: root is not an object");
  }
  if (!document.HasMember("common")) {
    return arrow::Status::ExecutionError("Invalid tea-config.json flie: root does not contain 'common' field");
  }
  if (auto status = ReadValues(&document["common"], config, ""); !status.ok()) {
    return status;
  }
  if (document.HasMember("profile-to-tables-path")) {
    if (!document["profile-to-tables-path"].IsString()) {
      return arrow::Status::ExecutionError(
          "Invalid tea-config.json flie: root has field 'profile-to-tables-path', but it is not string");
    }
    config->profile_to_tables_path = document["profile-to-tables-path"].GetString();
  }
  constexpr const char* kMustReadProfileConfig = "must-read-profile-to-tables-config";
  if (document.HasMember(kMustReadProfileConfig)) {
    if (!document[kMustReadProfileConfig].IsBool()) {
      return arrow::Status::ExecutionError("Invalid tea-config.json flie: root has field '",
                                           std::string(kMustReadProfileConfig), "', but it is not bool");
    }
    config->must_read_profile_to_tables_file = document[kMustReadProfileConfig].GetBool();
  }
  if (!profile.empty()) {
    if (!document.HasMember("profiles")) {
      return arrow::Status::OK();
    }
    if (!document["profiles"].IsObject()) {
      return arrow::Status::OK();
    }
    if (!document["profiles"].HasMember(profile.data())) {
      return arrow::Status::OK();
    }
    return ReadValues(&document["profiles"][profile.data()], config, "");
  }
  return arrow::Status::OK();
}

}  // namespace

TableId TableId::FromString(std::string_view s) {
  auto delim_pos = s.rfind('.');
  if (delim_pos == std::string::npos) {
    throw arrow::Status::ExecutionError("Invalid Iceberg table ID ", s, ", expected at least one '.' delimeter");
  }
  return {std::string(s, 0, delim_pos), std::string(s, delim_pos + 1, s.size() - delim_pos - 1)};
}

std::string TableId::ToString() const { return absl::StrCat(db_name, ".", table_name); }

arrow::Result<std::string> Config::GetJsonFilePath() {
  auto gphome = std::getenv("GPHOME");
  if (!gphome) {
    return arrow::Status::ExecutionError("GPHOME environment variable not set");
  }
  return absl::StrCat(gphome, "/tea/tea-config.json");
}

arrow::Result<std::string> Config::GetJsonSchemaFilePath() {
  auto gphome = std::getenv("GPHOME");
  if (!gphome) {
    return arrow::Status::ExecutionError("GPHOME environment variable not set");
  }
  return absl::StrCat(gphome, "/tea/tea-config-schema.json");
}

arrow::Status Config::FromJsonStream(std::istream& config, const std::optional<std::string>& schema_data,
                                     std::string_view profile) {
  rapidjson::IStreamWrapper isw(config);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  if (schema_data) {
    std::istringstream schema_input(*schema_data);

    rapidjson::IStreamWrapper schema_isw(schema_input);
    rapidjson::Document schema_doc;
    schema_doc.ParseStream(schema_isw);

    rapidjson::SchemaDocument schema(schema_doc);
    rapidjson::SchemaValidator validator(schema);

    if (!doc.Accept(validator)) {
      return arrow::Status::ExecutionError("JSON config validation error");
    }
  }

  return Load(doc, profile, this);
}

arrow::Status Config::FromJsonString(const std::string& data, const std::optional<std::string>& schema_data,
                                     std::string_view profile) {
  std::istringstream input(data);
  return FromJsonStream(input, schema_data, profile);
}

inline std::string ReadFile(std::istream& is) {
  std::stringstream ss;
  ss << is.rdbuf();
  return ss.str();
}

arrow::Status Config::FromJsonFile(const std::string& file_path, const std::optional<std::string>& file_schema_path,
                                   std::string_view profile) {
  std::ifstream input_config(file_path);
  if (!input_config.is_open()) {
    return arrow::Status::ExecutionError("Could not open file ", file_path, " for reading");
  }

  std::optional<std::string> schema_content;
  if (file_schema_path.has_value()) {
    std::ifstream input_schema(*file_schema_path);
    if (!input_schema.is_open()) {
      return arrow::Status::ExecutionError("Could not open file ", *file_schema_path, " for reading");
    }

    schema_content = ReadFile(input_schema);
  }

  return FromJsonStream(input_config, schema_content, profile);
}

Config ConfigSource::GetConfig(std::string_view profile) {
  auto json_config_path = Config::GetJsonFilePath();
  auto json_schema_config_path = Config::GetJsonSchemaFilePath();

  if (!json_config_path.ok()) {
    throw json_config_path.status();
  }

  std::optional<std::string> result_file_schema_path;
  if (std::filesystem::exists(*json_schema_config_path)) {
    result_file_schema_path = *json_schema_config_path;
  } else {
    result_file_schema_path = std::nullopt;
  }

  if (!std::filesystem::exists(*json_config_path)) {
    throw arrow::Status::ExecutionError("Cannot load configuration file ", *json_config_path);
  }
  Config config;
  LoadEnvDefaults(&config);
  if (auto status = config.FromJsonFile(*json_config_path, result_file_schema_path, profile); !status.ok()) {
    TEA_LOG("Incorrect json config " + status.message());
    throw arrow::Status::ExecutionError("Incorrect configuration file ", *json_config_path);
  }
  return config;
}

TableConfig ConfigSource::GetTableConfig(std::string_view url, const std::string& overrided_profile) {
  if (!url.starts_with(kTeaSchema)) {
    throw arrow::Status::ExecutionError("Url must start with ", kTeaSchema, " but ", url, " found");
  }
  const auto nested_url = url.substr(kTeaSchema.size());
  const auto components = iceberg::SplitUrl(nested_url);
  std::string_view profile = overrided_profile;
  for (auto& [key, value] : components.params) {
    if (key == std::string_view("profile")) {
      profile = value;
    }
  }

  TableConfig table_config;
  table_config.config = GetConfig(profile);

  const auto schema =
      (components.schema.empty()) ? table_config.config.meta_access.default_schema : std::string(components.schema);

  if (schema.empty() || schema == "teapot") {
    std::string_view table_id;
    if (components.path.empty()) {
      table_id = components.location;
    } else {
      table_config.config.teapot.location = components.location;
      table_id = components.path.substr(1);
    }
    table_config.source = TeapotTable{.table_id = TableId::FromString(table_id)};
  } else if (schema == "iceberg") {
    table_config.source = IcebergTable{TableId::FromString(components.location)};
  } else if (schema == "special" && components.location == "empty") {
    table_config.source = EmptyTable{};
  } else if (schema == "special" && components.location == "iceberg_tables_metrics") {
    table_config.source = IcebergMetricsTable{};
  } else if (schema == "file" || schema == "s3") {
    table_config.source =
        FileTable{.url = absl::StrCat(components.schema, "://", components.location, components.path)};
  } else {
    throw arrow::Status::ExecutionError("Invalid table url: ", url);
  }

  return table_config;
}

arrow::Result<std::unordered_map<std::string, std::string>> GetTableToProfileMapping(const std::string& file_content) {
  std::unordered_map<std::string, std::string> result;

  rapidjson::Document doc;
  doc.Parse(file_content.data(), file_content.size());

  if (doc.HasParseError()) {
    return arrow::Status::ExecutionError("Profile-to-table parsing error: not a valid JSON");
  }
  if (!doc.IsObject()) {
    return arrow::Status::ExecutionError("Profile-to-table parsing error: root is not an object");
  }
  if (!doc.HasMember("profile-to-tables")) {
    return arrow::Status::ExecutionError(
        "Profile-to-table parsing error: field 'profile-to-tables' is expected but not found");
  }
  const auto& profile_to_tables = doc["profile-to-tables"];

  std::unordered_set<std::string> bad_tables;

  for (auto iter = profile_to_tables.MemberBegin(); iter != profile_to_tables.MemberEnd(); ++iter) {
    const auto& key = iter->name;
    const auto& value = iter->value;
    if (!key.IsString()) {
      return arrow::Status::ExecutionError("Profile-to-table parsing error: there is a key that is not a string");
    }

    const std::string profile = key.GetString();

    if (!value.IsArray()) {
      return arrow::Status::ExecutionError("Profile-to-table parsing error: value for profile '", profile,
                                           "' is not an array");
    }

    const auto& tables = value.GetArray();

    for (const auto& elem : tables) {
      if (!elem.IsString()) {
        return arrow::Status::ExecutionError("Profile-to-table parsing error: element for key '", profile,
                                             "' is not a string");
      }

      std::string table_name = elem.GetString();

      if (result.contains(table_name)) {
        bad_tables.insert(table_name);
        continue;
      }
      result[table_name] = profile;
    }
  }

  for (const auto& bad_table : bad_tables) {
    result.erase(bad_table);
  }

  return result;
}

}  // namespace tea
