#include <iceberg/schema.h>

#include <chrono>
#include <exception>
#include <limits>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "arrow/status.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/uuid.h"

#include "tea/common/config.h"
#include "tea/metadata/access_iceberg.h"
#include "tea/samovar/network_layer/redis_client.h"
#include "tea/samovar/planner.h"
#include "tea/samovar/utils.h"
#include "tea/util/measure.h"

std::shared_ptr<iceberg::IFileSystemProvider> MakeFileSystemProvider(const tea::Config &config) {
  iceberg::S3FileSystemGetter::Config s3_input_cfg;
  s3_input_cfg.access_key = config.s3.access_key;
  s3_input_cfg.secret_key = config.s3.secret_key;
  s3_input_cfg.region = config.s3.region;
  s3_input_cfg.scheme = config.s3.scheme;
  s3_input_cfg.endpoint_override = config.s3.endpoint_override;

  s3_input_cfg.connect_timeout = config.s3.connect_timeout;
  s3_input_cfg.request_timeout = config.s3.request_timeout;

  s3_input_cfg.retry_max_attempts = config.s3.retry_max_attempts;

  std::shared_ptr<iceberg::IFileSystemGetter> s3_fs_getter =
      std::make_shared<iceberg::S3FileSystemGetter>(s3_input_cfg);

  std::shared_ptr<iceberg::IFileSystemGetter> local_fs_getter = std::make_shared<iceberg::LocalFileSystemGetter>();

  std::map<std::string, std::shared_ptr<iceberg::IFileSystemGetter>> schema_to_fs_builder = {
      {"file", local_fs_getter}, {"s3", s3_fs_getter}, {"s3a", s3_fs_getter}};

  return std::make_shared<iceberg::FileSystemProvider>(std::move(schema_to_fs_builder));
}

void PrintPlannerStats(const tea::PlannerStats &stats) {
  std::cout << "Stats:" << std::endl;
  std::cout << "  samovar_initial_tasks_count: " << stats.samovar_initial_tasks_count << std::endl;
  std::cout << "  samovar_splitted_tasks_count: " << stats.samovar_splitted_tasks_count << std::endl;
  std::cout << "  iceberg_bytes_read: " << stats.iceberg_bytes_read << std::endl;
  std::cout << "  iceberg_requests: " << stats.iceberg_requests << std::endl;
  std::cout << "  iceberg_files_read: " << stats.iceberg_files_read << std::endl;
  std::cout << "  catalog_connections_established: " << stats.catalog_connections_established << std::endl;
  std::cout << "  iceberg_fs_duration_ticks: " << stats.iceberg_fs_duration << std::endl;
  std::cout << "  plan_duration_ticks: " << stats.plan_duration << std::endl;
}

ABSL_FLAG(std::string, hms_host, "localhost", "host to connect to");
ABSL_FLAG(uint16_t, hms_port, 9083, "port to connect to");

ABSL_FLAG(int, num_uploads, 1, "num_uploads");
ABSL_FLAG(int, segment_id, 0, "segment_id");
ABSL_FLAG(int, segment_count, 1, "segment_count");
ABSL_FLAG(std::string, db_name, "", "db_name");
ABSL_FLAG(std::string, table_id, "", "table_id");

ABSL_FLAG(std::string, s3_access_key_id, "", "s3_key_id");
ABSL_FLAG(std::string, s3_secret_access_key, "", "s3_access_key");
ABSL_FLAG(std::string, s3_endpoint, "", "s3_endpoint");

ABSL_FLAG(std::string, redis_host, "localhost", "host to connect to");
ABSL_FLAG(uint16_t, redis_port, 6379, "port to connect to");

ABSL_FLAG(std::string, location, "", "location of metadata");
ABSL_FLAG(std::string, hms_table, "", "hms table name");
ABSL_FLAG(std::string, hms_db, "", "hms db name");

ABSL_FLAG(bool, use_avro_reader_schema, false, "");

int main(int argc, char **argv) {
  try {
    absl::ParseCommandLine(argc, argv);

    auto redis_client = tea::samovar::RedisClient(
        std::vector{tea::Endpoint{.host = absl::GetFlag(FLAGS_redis_host), .port = absl::GetFlag(FLAGS_redis_port)}},
        std::chrono::milliseconds(30000), std::chrono::milliseconds(30000));
    {
      auto result = redis_client.SendRequest({"INFO", "memory"});
      std::cout << "Start usage memory in redis " << result.Get()->str << '\n';
    }

    auto config = tea::Config{};
    config.catalog.hms_endpoints =
        std::vector{tea::Endpoint{.host = absl::GetFlag(FLAGS_hms_host), .port = absl::GetFlag(FLAGS_hms_port)}};
    config.samovar_config.endpoints =
        std::vector{tea::Endpoint{.host = absl::GetFlag(FLAGS_redis_host), .port = absl::GetFlag(FLAGS_redis_port)}};

    config.s3.access_key = absl::GetFlag(FLAGS_s3_access_key_id);
    config.s3.secret_key = absl::GetFlag(FLAGS_s3_secret_access_key);
    config.s3.endpoint_override = absl::GetFlag(FLAGS_s3_endpoint);

    bool use_avro_reader_schema = absl::GetFlag(FLAGS_use_avro_reader_schema);
    if (use_avro_reader_schema) {
      config.features.use_avro_projection_minimum_columns = 0;
    } else {
      config.features.use_avro_projection_minimum_columns = std::numeric_limits<uint64_t>::max();
    }

    auto table_id = absl::GetFlag(FLAGS_table_id);
    auto db_name = absl::GetFlag(FLAGS_db_name);

    std::optional<std::string> real_location;
    auto hms_host = absl::GetFlag(FLAGS_hms_host);
    auto hms_location = absl::GetFlag(FLAGS_location);
    auto hms_db = absl::GetFlag(FLAGS_hms_db);
    auto hms_table = absl::GetFlag(FLAGS_hms_table);
    if (hms_host.empty() && hms_table.empty()) {
      real_location = hms_location;
      std::cerr << "real_location = " << *real_location << std::endl;
    } else if (!hms_location.empty()) {
      throw std::runtime_error("Only one of (hms host, hms table) and (hms location) should be setted");
    }

    iceberg::UuidGenerator generator;
    int num_iterations = absl::GetFlag(FLAGS_num_uploads);
    for (int i = 0; i < num_iterations; ++i) {
      std::cerr << "[" << i << '/' << num_iterations << "]\n";
      const std::string queue_name = tea::samovar::MakeSessionIdentifier(
          tea::IcebergTable{.table_id = tea::TableId{.db_name = db_name, .table_name = table_id}}, "cluster_id",
          "session_id", generator.CreateRandom().ToString(), 0, 1, false);

      auto fs_provider = MakeFileSystemProvider(config);

      iceberg::ice_tea::ScanMetadata iceberg_meta;
      tea::PlannerStats stats;

      tea::TimerTicks ticks;
      tea::TimerClock clock;

      std::cerr << "Getting metadata" << std::endl;
      tea::CancelToken cancel_token;

      if (!real_location) {
        auto iceberg_result =
            tea::meta::access::FromIceberg(config, tea::TableId{.db_name = hms_db, .table_name = hms_table}, nullptr,
                                           fs_provider, 0, nullptr, cancel_token);

        iceberg_meta = std::move(iceberg_result.first);
        stats = std::move(iceberg_result.second);
      } else {
        auto iceberg_result = tea::meta::access::FromIcebergWithLocation(
            nullptr, fs_provider, *real_location, 0,
            [&](const iceberg::Schema &schema) { return use_avro_reader_schema; }, nullptr, cancel_token);

        iceberg_meta = std::move(iceberg_result.first);
        stats = std::move(iceberg_result.second);
      }

      double ticks_in_second = static_cast<double>(ticks.duration()) /
                               std::chrono::duration_cast<std::chrono::nanoseconds>(clock.duration()).count();
      std::cerr << "Ticks in second: " << ticks_in_second << std::endl;

      std::cerr << "Filling samovar" << std::endl;
      auto maybe_stats = tea::samovar::FillSamovar(config, std::move(iceberg_meta), absl::GetFlag(FLAGS_segment_id),
                                                   absl::GetFlag(FLAGS_segment_count), queue_name, "", cancel_token);
      if (!maybe_stats.ok()) {
        std::cerr << "Failed to fill samovar: " << maybe_stats.status().message() << std::endl;
      }

      PrintPlannerStats(stats);
    }
    {
      auto result = redis_client.SendRequest({"INFO", "memory"});
      std::cout << "Result usage memory in redis " << result.Get()->str << '\n';
    }
    return 0;
  } catch (arrow::Status &s) {
    std::cerr << s.ToString() << std::endl;
  } catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
  }
  return 1;
}
