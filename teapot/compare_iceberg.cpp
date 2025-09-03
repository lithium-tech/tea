#include <iceberg/tea_scan.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/absl_log.h"
#include "absl/log/initialize.h"
#include "grpcpp/grpcpp.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/experimental_representations.h"

#include "tea/common/utils.h"
#include "tea/metadata/access_iceberg.h"
#include "teapot/teapot.grpc.pb.h"

std::shared_ptr<iceberg::IFileSystemProvider> MakeFileSystemProvider(const tea::Config& config) {
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

void PrintScanMetadata(const iceberg::ice_tea::ScanMetadata& scan_meta) {
  std::cout << "Metadata:\n";
  for (size_t i = 0; i < scan_meta.partitions.size(); ++i) {
    for (size_t j = 0; j < scan_meta.partitions[i].size(); ++j) {
      std::cout << "Partition " << i << " layer " << j << " content:\n";
      for (const auto& entry : scan_meta.partitions[i][j].data_entries_) {
        std::cout << "data entry " << entry.path << '\n';
        for (auto segment : entry.parts) {
          std::cout << "segment " << segment.offset << ' ' << segment.length << '\n';
        }
      }
      for (const auto& entry : scan_meta.partitions[i][j].positional_delete_entries_) {
        std::cout << "positional delete " << entry.path << '\n';
      }
      for (const auto& entry : scan_meta.partitions[i][j].equality_delete_entries_) {
        std::cout << "equality delete " << entry.path << '\n';
      }
    }
  }
  std::cout << "Schema:\n";
  for (const auto& column : scan_meta.schema->Columns()) {
    std::cout << "{'field_id': " << column.field_id << ", 'is_required': " << column.is_required
              << ", 'name': " << column.name << ", 'type': " << column.type->ToString() << "}\n";
  }
}

/// TEAPOT parameters
ABSL_FLAG(std::string, teapot_endpoint, "localhost:50051", "host:port to connect to");
ABSL_FLAG(int, max_message_size, 1024 * 1024 * 100, "max_message_size");
ABSL_FLAG(int, timeout_seconds, 100, "timeout_seconds");

ABSL_FLAG(std::string, session_id, "", "session_id");
ABSL_FLAG(int, segment_id, 0, "segment_id");
ABSL_FLAG(int, segment_count, 1, "segment_count");
ABSL_FLAG(std::optional<std::string>, table_id, std::nullopt, "table_id");
ABSL_FLAG(std::optional<std::string>, filter, std::nullopt, "Iceberg expresions JSON to use as a filter");

/// Iceberg parameters
ABSL_FLAG(std::string, hms_host, "", "hms host");
ABSL_FLAG(int, hms_port, 9083, "hms port");
ABSL_FLAG(std::string, hms_table, "", "hms table name");
ABSL_FLAG(std::string, hms_db, "", "hms db name");

/// NOTE: this field should be specified if hms parameters are not setted.
ABSL_FLAG(std::string, location, "", "location of metadata");

ABSL_FLAG(std::string, s3_access_key_id, "", "s3_key_id");
ABSL_FLAG(std::string, s3_secret_access_key, "", "s3_access_key");
ABSL_FLAG(std::string, s3_endpoint, "", "s3_endpoint");

ABSL_FLAG(bool, use_avro_reader_schema, false, "");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();

  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(absl::GetFlag(FLAGS_max_message_size));
  auto creds = grpc::InsecureChannelCredentials();
  auto channel = grpc::CreateCustomChannel(absl::GetFlag(FLAGS_teapot_endpoint), creds, args);
  auto client = teapot::Teapot::NewStub(channel);
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(absl::GetFlag(FLAGS_timeout_seconds)));

  teapot::MetadataRequest request;
  request.set_session_id(absl::GetFlag(FLAGS_session_id));
  request.set_segment_id(absl::GetFlag(FLAGS_segment_id));
  request.set_segment_count(absl::GetFlag(FLAGS_segment_count));
  if (auto opt = absl::GetFlag(FLAGS_table_id)) {
    request.set_table_id(*opt);
  }
  if (auto opt = absl::GetFlag(FLAGS_filter)) {
    request.set_iceberg_expression_json(*opt);
  }

  teapot::MetadataResponse reply;
  grpc::Status status = client->GetMetadata(&context, request, &reply);
  if (!status.ok()) {
    ABSL_LOG(ERROR) << "RPC failed: " << status.error_code() << " " << status.error_message();
    return 1;
  }
  if (!reply.has_result()) {
    const auto& error = reply.error();
    ABSL_LOG(ERROR) << "Teapot error(" << error.error_code() << "): " << error.error_message();
    return 1;
  }

  ABSL_LOG(INFO) << "Got teapot.MetadataResponseResult";
  const auto& result = reply.result();

  std::optional<std::string> real_location;
  auto hms_host = absl::GetFlag(FLAGS_hms_host);
  auto hms_db = absl::GetFlag(FLAGS_hms_db);
  auto hms_table = absl::GetFlag(FLAGS_hms_table);
  auto hms_location = absl::GetFlag(FLAGS_location);
  if (hms_host.empty() && hms_table.empty()) {
    real_location = hms_location;
  } else if (!hms_location.empty()) {
    throw std::runtime_error("Only one of (hms host, hms table) and (hms location) should be setted");
  }

  auto config = tea::Config{};
  config.catalog.hms_endpoints = std::vector{tea::Endpoint{
      .host = absl::GetFlag(FLAGS_hms_host), .port = static_cast<uint16_t>(absl::GetFlag(FLAGS_hms_port))}};

  config.s3.access_key = absl::GetFlag(FLAGS_s3_access_key_id);
  config.s3.secret_key = absl::GetFlag(FLAGS_s3_secret_access_key);
  config.s3.endpoint_override = absl::GetFlag(FLAGS_s3_endpoint);

  bool use_avro_reader_schema = absl::GetFlag(FLAGS_use_avro_reader_schema);
  if (use_avro_reader_schema) {
    config.features.use_avro_projection_minimum_columns = 0;
  } else {
    config.features.use_avro_projection_minimum_columns = std::numeric_limits<uint64_t>::max();
  }

  auto fs_provider = MakeFileSystemProvider(config);
  iceberg::ice_tea::ScanMetadata iceberg_meta;
  tea::PlannerStats stats;

  if (!real_location) {
    auto iceberg_result = tea::meta::access::FromIceberg(
        config, tea::TableId{.db_name = hms_db, .table_name = hms_table}, nullptr, fs_provider, 0, nullptr);

    iceberg_meta = std::move(iceberg_result.first);
    stats = std::move(iceberg_result.second);
  } else {
    auto iceberg_result = tea::meta::access::FromIcebergWithLocation(
        nullptr, fs_provider, *real_location, 0, [&](const iceberg::Schema& schema) { return use_avro_reader_schema; },
        nullptr);

    iceberg_meta = std::move(iceberg_result.first);
    stats = std::move(iceberg_result.second);
  }

  auto teapot_meta = tea::MetadataResponseResultToScanMetadata(result);

  if (!iceberg::experimental::AreScanMetadataEqual(iceberg_meta, teapot_meta)) {
    std::cout << "Iceberg metadata:\n";
    PrintScanMetadata(iceberg_meta);
    std::cout << "Teapot metadata:\n";
    PrintScanMetadata(teapot_meta);

    throw std::runtime_error("Metadata from iceberg and teapot are not equals");
  }
  std::cout << "Metadata from iceberg and teapot are equals\n";
  return 0;
}
