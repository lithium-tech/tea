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

#include "teapot/teapot.grpc.pb.h"

ABSL_FLAG(std::string, host, "localhost:50051", "host:port to connect to");
ABSL_FLAG(int, max_message_size, 1024 * 1024 * 100, "max_message_size");
ABSL_FLAG(int, timeout_seconds, 100, "timeout_seconds");

ABSL_FLAG(std::string, request_type, "get", "request type. one of: get, delete");
ABSL_FLAG(std::string, session_id, "", "session_id");
ABSL_FLAG(int, segment_id, 0, "segment_id");
ABSL_FLAG(int, segment_count, 1, "segment_count");
ABSL_FLAG(std::optional<std::string>, table_id, std::nullopt, "table_id");
ABSL_FLAG(std::optional<std::string>, filter, std::nullopt, "Iceberg expresions JSON to use as a filter");
ABSL_FLAG(std::optional<std::string>, out_file, std::nullopt, "output proto file");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();

  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(absl::GetFlag(FLAGS_max_message_size));
  auto creds = grpc::InsecureChannelCredentials();
  auto channel = grpc::CreateCustomChannel(absl::GetFlag(FLAGS_host), creds, args);
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

  auto output_file = absl::GetFlag(FLAGS_out_file);
  auto request_type = absl::GetFlag(FLAGS_request_type);

  grpc::Status status;
  teapot::MetadataResponse reply;
  if (request_type == "get") {
    status = client->GetMetadata(&context, request, &reply);
  } else if (request_type == "delete") {
    status = client->DeleteMetadata(&context, request, &reply);
  } else {
    ABSL_LOG(ERROR) << "Unknown request type: " << request_type;
    return 1;
  }
  if (!status.ok()) {
    ABSL_LOG(ERROR) << "RPC failed: " << status.error_code() << " " << status.error_message();
    return 1;
  }
  if (reply.has_error()) {
    const auto& error = reply.error();
    ABSL_LOG(ERROR) << "Teapot error(" << error.error_code() << "): " << error.error_message();
    return 1;
  }

  ABSL_LOG(INFO) << "Got teapot.MetadataResponseResult";
  const auto& result = reply.result();
  if (output_file) {
    std::ofstream out(*output_file);
    result.SerializeToOstream(&out);
  }
  std::cout << result.DebugString() << std::endl;
  return 0;
}
