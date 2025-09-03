#include "tea/metadata/access_teapot.h"

#include <memory>
#include <string>
#include <utility>

#include "arrow/status.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/support/status.h"
#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
#include "tea/common/utils.h"
#include "tea/observability/planner_stats.h"
#include "tea/observability/tea_log.h"
#include "tea/util/measure.h"
#include "tea/util/signal_blocker.h"
#include "teapot/teapot.grpc.pb.h"
#include "teapot/teapot.pb.h"

namespace tea::meta::access {

std::pair<iceberg::ice_tea::ScanMetadata, PlannerStats> FromTeapot(const Config& config, const std::string& table_name,
                                                                   const std::string& session_id, int segment_id,
                                                                   int segment_count,
                                                                   const std::string& teapot_file_filter_string) {
  tea::SignalBlocker signal_blocker;
  PlannerStats stats;
  std::optional<ScopedTimerTicks> timer = ScopedTimerTicks(stats.plan_duration);

  grpc::ChannelArguments options;
  if (config.limits.grpc_max_message_size != 0) {
    options.SetMaxReceiveMessageSize(config.limits.grpc_max_message_size);
  }
  auto channel = grpc::CreateCustomChannel(config.teapot.location, grpc::InsecureChannelCredentials(), options);
  auto client_stub = teapot::Teapot::NewStub(channel);

  auto request = [&]() {
    teapot::MetadataRequest request;
    request.set_table_id(table_name);
    request.set_session_id(session_id);
    request.set_segment_id(segment_id);
    request.set_segment_count(segment_count);
    request.set_iceberg_expression_json(teapot_file_filter_string);

    if (config.teapot.location.empty()) {
      throw arrow::Status::ExecutionError("No teapot location specified");
    }

    return request;
  }();

  if (!request.iceberg_expression_json().empty()) {
    TEA_LOG("Using teapot filter: " + request.iceberg_expression_json());
  } else {
    TEA_LOG("No teapot filter");
  }
  teapot::MetadataResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + config.teapot.timeout);
  if (auto status = client_stub->GetMetadata(&context, request, &response); !status.ok()) {
    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      std::stringstream timeout_as_str;
      timeout_as_str << config.teapot.timeout.count() << "ms";
      throw arrow::Status::ExecutionError("Teapot error: (at ", config.teapot.location, ", table_name '", table_name,
                                          "') ", status.error_code(), " ", status.error_message(),
                                          " (Teapot did not return metadata within ", timeout_as_str.str(), ")");
    } else {
      throw arrow::Status::ExecutionError("Teapot error: (at ", config.teapot.location, ", table_name '", table_name,
                                          "') ", status.error_code(), " ", status.error_message());
    }
  }

  if (response.has_result()) {
    TEA_LOG("Teapot response size = " + std::to_string(response.ByteSizeLong()));
    auto resp = MetadataResponseResultToScanMetadata(response.result());
    if (config.features.optimize_deletes_in_teapot_response) {
      resp = DeletePlanner::OptimizeScanMetadata(std::move(resp));
    }
    timer.reset();
    return std::make_pair(std::move(resp), std::move(stats));
  } else {
    throw arrow::Status::ExecutionError("Teapot error: (at ", config.teapot.location, ", table_name '", table_name,
                                        "') ", response.error().error_code(), " ", response.error().error_message());
  }
}

}  // namespace tea::meta::access
