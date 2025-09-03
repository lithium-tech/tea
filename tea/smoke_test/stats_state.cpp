#include "tea/smoke_test/stats_state.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "grpcpp/grpcpp.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"

#include "tea/debug/stats_state.grpc.pb.h"
#include "tea/debug/stats_state.pb.h"

namespace tea {
namespace {

using grpc::Server;
using grpc::ServerContext;
using grpc::Status;
using stats_state::GandivaFilterRequest;
using stats_state::Response;
using stats_state::StatsRequest;

static const char* kDefaultHostPort = "localhost:50003";
}  // namespace

struct StatsState::StatsStateImpl final : public stats_state::StatsState::Service {
  Status SetGandivaFilter(ServerContext* context, const GandivaFilterRequest* request, Response* response) override {
    std::lock_guard<std::mutex> lock(mutex_);
    gandiva_filters_.emplace_back(request->gandiva_filter());
    return Status::OK;
  }

  Status SetPotentialRowGroupFilter(ServerContext* context, const GandivaFilterRequest* request,
                                    Response* response) override {
    std::lock_guard<std::mutex> lock(mutex_);
    potential_row_group_filter_ = request->gandiva_filter();
    return Status::OK;
  }

  Status SetStats(ServerContext* context, const StatsRequest* request, Response* response) override {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.emplace_back(*request);
    return Status::OK;
  }

  std::mutex mutex_;
  std::vector<std::string> gandiva_filters_;
  std::string potential_row_group_filter_;
  std::vector<stats_state::StatsRequest> stats_;
  std::unique_ptr<grpc::Server> server_;
};

StatsState::StatsState() : impl_(std::make_unique<StatsStateImpl>()) {
  impl_->server_ = grpc::ServerBuilder()
                       .AddListeningPort(kDefaultHostPort, grpc::InsecureServerCredentials())
                       .RegisterService(impl_.get())
                       .BuildAndStart();
}

StatsState::~StatsState() {
  impl_->server_->Shutdown(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
}

std::string StatsState::GetLastGandivaFilter() const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  auto filters = std::move(impl_->gandiva_filters_);
  if (filters.empty()) {
    return "";
  } else {
    return filters[0];
  }
}

std::vector<std::string> StatsState::GetAllGandivaFilters() const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  auto filters = std::move(impl_->gandiva_filters_);
  std::sort(filters.begin(), filters.end());
  filters.erase(std::unique(filters.begin(), filters.end()), filters.end());
  return filters;
}

std::string StatsState::GetPotentialRowGroupFilter() const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  return std::move(impl_->potential_row_group_filter_);
}

void StatsState::ClearFilters() {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  impl_->gandiva_filters_.clear();
}

void StatsState::ClearStats() {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  impl_->stats_.clear();
}

std::vector<stats_state::ExecutionStats> StatsState::GetStats(bool include_master) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  std::vector<stats_state::StatsRequest> requests = std::move(impl_->stats_);
  std::vector<stats_state::ExecutionStats> result;
  for (const auto& request : requests) {
    if (!include_master && request.scan_id().segment_id() == -1) {
      continue;
    }
    result.emplace_back(request.stats());
  }
  return result;
}

std::string StatsState::GetHostPort() const { return kDefaultHostPort; }

}  // namespace tea
