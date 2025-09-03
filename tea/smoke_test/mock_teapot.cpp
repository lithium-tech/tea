#include "tea/smoke_test/mock_teapot.h"

#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <utility>

#include "grpcpp/grpcpp.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"

namespace tea {

static constexpr std::string_view kDefaultHost = "localhost";
static constexpr int kDefaultPort = 50002;

struct MockTeapot::MockTeapotImpl final : public teapot::Teapot::Service {
  grpc::Status GetMetadata(grpc::ServerContext* context, const teapot::MetadataRequest* request,
                           teapot::MetadataResponse* response) override {
    std::lock_guard<std::mutex> lock(response_mutex_);
    if (!responses_.contains(request->table_id())) {
      auto error = response->mutable_error();
      error->set_error_code(::teapot::ErrorCode::TABLE_NOT_FOUND);
      error->set_error_message(request->table_id() + " not found");
      return grpc::Status::OK;
    }
    const auto& prepared_response = responses_.at(request->table_id());

    if (request->segment_id() == -1) {
      *response = prepared_response;
      last_request_ = *request;
    } else {
      auto* result = response->mutable_result();
      *result->mutable_schema() = prepared_response.result().schema();
      int iter = 0;
      int seg_id = request->segment_id();
      int seg_count = request->segment_count();
      for (auto elem : prepared_response.result().fragments()) {
        if (iter % seg_count == seg_id) {
          auto* new_fragment = result->add_fragments();
          *new_fragment = elem;
        }
        ++iter;
      }

      last_request_ = *request;
    }
    return grpc::Status::OK;
  }

  grpc::Status DeleteMetadata(grpc::ServerContext* context, const teapot::MetadataRequest* request,
                              teapot::MetadataResponse* response) override {
    std::lock_guard<std::mutex> lock(response_mutex_);

    responses_.erase(request->table_id());

    response->clear_result();
    return grpc::Status::OK;
  }

  std::mutex response_mutex_;
  std::map<std::string, teapot::MetadataResponse> responses_;
  teapot::MetadataRequest last_request_;
  std::unique_ptr<grpc::Server> server_;
};

MockTeapot::MockTeapot() : impl_(std::make_unique<MockTeapotImpl>()) {
  impl_->server_ = grpc::ServerBuilder()
                       .AddListeningPort(std::string(kDefaultHost) + ":" + std::to_string(kDefaultPort),
                                         grpc::InsecureServerCredentials())
                       .RegisterService(impl_.get())
                       .BuildAndStart();
}

MockTeapot::~MockTeapot() {
  impl_->server_->Shutdown(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
}

void MockTeapot::ClearResponse(const std::string& table_id) {
  std::lock_guard<std::mutex> lock(impl_->response_mutex_);
  impl_->responses_.erase(table_id);
}

void MockTeapot::ClearResponses() {
  std::lock_guard<std::mutex> lock(impl_->response_mutex_);
  impl_->responses_.clear();
}

void MockTeapot::SetResponse(const std::string& table_id, teapot::MetadataResponse response) {
  std::lock_guard<std::mutex> lock(impl_->response_mutex_);
  impl_->responses_[table_id] = std::move(response);
}

teapot::MetadataRequest MockTeapot::GetLastRequest() const {
  std::lock_guard<std::mutex> lock(impl_->response_mutex_);
  return impl_->last_request_;
}

std::lock_guard<std::mutex> MockTeapot::Lock() const { return std::lock_guard<std::mutex>(impl_->response_mutex_); }

std::string MockTeapot::GetHost() const { return std::string(kDefaultHost); }

int MockTeapot::GetPort() const { return kDefaultPort; }

}  // namespace tea
