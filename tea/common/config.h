#pragma once

#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/common/fs/filesystem_provider.h"

namespace tea {

struct S3Config {
  std::string endpoint_override;
  std::string scheme = "https";
  std::string region;
  std::string access_key;
  std::string secret_key;
  std::chrono::milliseconds connect_timeout = std::chrono::milliseconds(1000);
  std::chrono::milliseconds request_timeout = std::chrono::milliseconds(3000);
  uint64_t retry_max_attempts = 3;

  bool operator==(const S3Config&) const = default;
};

struct Endpoint {
  std::string host;
  uint16_t port;

  bool operator==(const Endpoint&) const = default;
};

struct CatalogConfig {
  enum class CatalogType {
    kREST,
    kHMS,
  } type = CatalogType::kHMS;

  std::vector<Endpoint> hms_endpoints;
  std::vector<Endpoint> rest_endpoints;

  bool operator==(const CatalogConfig&) const = default;
};

/// COMPAT(scanhex12)
struct HMSCatalog {
  std::vector<Endpoint> hms_endpoints;

  bool operator==(const HMSCatalog&) const = default;
};

struct TeapotConfig {
  std::string location;
  std::chrono::milliseconds timeout = std::chrono::milliseconds(5000);

  bool operator==(const TeapotConfig&) const = default;
};

struct Limits {
  uint64_t max_cpu_threads = 1;
  uint64_t max_io_threads = 1;
  uint64_t parquet_buffer_size = 1ull << 20;
  uint64_t arrow_buffer_rows = 1ull << 16;
  uint64_t adaptive_batch_max_rows = 1ull << 16;
  uint64_t adaptive_batch_min_rows = 1ull << 7;
  uint64_t adaptive_batch_max_bytes_in_batch = 128ull << 20;
  uint64_t adaptive_batch_max_bytes_in_column = 4ull << 20;
  uint64_t equality_delete_max_rows = 0;
  uint64_t equality_delete_max_mb_size = 1ull << 20;
  uint64_t metadata_cache_size = 0;
  uint64_t grpc_max_message_size = 16ull << 20;
  uint64_t json_max_message_size_on_master = 32ull << 20;

  bool operator==(const Limits&) const = default;
};

struct JsonConfig {
  bool substitute_unicode_if_json_type = true;

  bool operator==(const JsonConfig&) const = default;
};

struct MetadataAccess {
  std::string default_schema;

  bool operator==(const MetadataAccess&) const = default;
};

struct Features {
  bool prefetch = false;
  bool read_in_multiple_threads = false;
  bool enable_row_group_filters = false;
  bool postfilter_on_gp = false;
  bool use_custom_heap_form_tuple = false;
  bool use_virtual_tuple = false;
  bool use_specialized_deletes = true;
  bool substitute_illegal_code_points = true;
  bool ext_table_filter_walker_for_projection = true;
  bool throw_if_memory_limit_exceeded = false;
  bool use_helper_thread = false;
  bool optimize_deletes_in_teapot_response = false;
  bool use_iceberg_metadata_partition_pruning = true;
  bool use_adaptive_batch_size = true;
  uint64_t use_avro_projection_minimum_columns = 20;
  std::vector<int32_t> filter_ignored_op_exprs;
  std::vector<int32_t> filter_ignored_func_exprs;

  bool operator==(const Features&) const = default;
};

enum class BackoffType { kNoBackoff, kLinearBackoff, kExponentialBackoff };

/* Split type is a samovar mode for splitting data entries
 *  - kOffsets means task will be split by split offsets
 *  - kWholeDataEntry means task will not be split
 */
enum class SplitType { kOffsets, kWholeDataEntry };

enum class BalancerType { kOneQueue };

struct SamovarConfig {
  bool turn_on_samovar = false;

  BackoffType backoff_type = BackoffType::kNoBackoff;
  std::chrono::milliseconds linear_backoff_time_to_sleep_ms = std::chrono::milliseconds(0);
  std::optional<double> exponential_backoff_sleep_coef = std::nullopt;
  std::optional<std::chrono::milliseconds> exponential_backoff_limit = std::nullopt;
  int limit_retries = 0;
  SplitType split_type = SplitType::kOffsets;
  BalancerType balancer_type = BalancerType::kOneQueue;
  int batch_size = 1;
  std::string cluster_id;
  std::string compressor_name;

  std::vector<Endpoint> endpoints;
  std::chrono::seconds ttl_seconds = std::chrono::seconds(std::numeric_limits<int32_t>::max());
  std::chrono::milliseconds request_timeout = std::chrono::milliseconds(5000);
  std::chrono::milliseconds connection_timeout = std::chrono::milliseconds(5000);

  std::optional<int> first_request_fragments = std::nullopt;

  std::unordered_set<int> work_segments = {};

  bool wait_before_processing = false;
  std::chrono::milliseconds min_time_before_processing_ms = std::chrono::milliseconds(0);
  std::chrono::milliseconds max_time_before_processing_ms = std::chrono::milliseconds(0);

  std::chrono::seconds ttl_utils_seconds = std::chrono::seconds(0);
  bool need_sync_on_init = true;
  bool allow_static_balancing = true;

  bool operator==(const SamovarConfig&) const = default;
};

struct Debug {
  bool test_gandiva_filter = false;
  bool test_stats = false;
  bool use_slow_filesystem = false;
  double slow_filesystem_delay_seconds = 0.0;
  uint64_t intermediate_logs_period_seconds = 60 * 60;

  bool operator==(const Debug&) const = default;
};

struct Config {
  std::string profile_to_tables_path;

  S3Config s3;
  CatalogConfig catalog;
  HMSCatalog hms_catalog;
  TeapotConfig teapot;
  Limits limits;
  Features features;
  Debug debug;
  JsonConfig json;
  SamovarConfig samovar_config;
  MetadataAccess meta_access;

  bool operator==(const Config&) const = default;

  arrow::Status FromJsonFile(const std::string& file_path, const std::optional<std::string>& file_schema_path,
                             std::string_view profile = std::string_view());
  arrow::Status FromJsonString(const std::string& file_path, const std::optional<std::string>& schema_data,
                               std::string_view profile = std::string_view());
  static arrow::Result<std::string> GetJsonFilePath();
  static arrow::Result<std::string> GetJsonSchemaFilePath();

 private:
  arrow::Status FromJsonStream(std::istream& config, const std::optional<std::string>& schema_data,
                               std::string_view profile);
};

struct TableId {
  std::string db_name;
  std::string table_name;

  static TableId FromString(std::string_view s);
  std::string ToString() const;

  auto operator<=>(const TableId&) const = default;
};

struct TeapotTable {
  TableId table_id;

  auto operator<=>(const TeapotTable&) const = default;
};

struct IcebergTable {
  TableId table_id;

  auto operator<=>(const IcebergTable&) const = default;
};

struct FileTable {
  std::string url;

  auto operator<=>(const FileTable&) const = default;
};

struct EmptyTable {
  auto operator<=>(const EmptyTable&) const = default;
};

struct IcebergMetricsTable {
  auto operator<=>(const IcebergMetricsTable&) const = default;
};

enum TableType { kEmpty, kTeapot, kIceberg, kFile, kTotalMetrics };
// TODO(hvintus): replace with proper interface
using TableSource = std::variant<EmptyTable, TeapotTable, IcebergTable, FileTable, IcebergMetricsTable>;

struct TableConfig {
  TableSource source;
  Config config;
};

class ConfigSource {
 public:
  static Config GetConfig(std::string_view profile = std::string_view());
  static TableConfig GetTableConfig(std::string_view url, const std::string& overrided_profile = "");
};

arrow::Result<std::unordered_map<std::string, std::string>> GetTableToProfileMapping(const std::string& file_content);

}  // namespace tea
