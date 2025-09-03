#pragma once

#include <memory>
#include <mutex>
#include <string>

#define LOGLEVEL_21 LOGLEVEL_INFO
#include "teapot/teapot.grpc.pb.h"

namespace tea {

class MockTeapot {
 public:
  MockTeapot();
  ~MockTeapot();
  MockTeapot(const MockTeapot&) = delete;
  MockTeapot& operator=(const MockTeapot&) = delete;

  void SetResponse(const std::string& table_name, teapot::MetadataResponse response);
  void ClearResponse(const std::string& table_name);
  void ClearResponses();
  teapot::MetadataRequest GetLastRequest() const;
  std::string GetHost() const;
  int GetPort() const;

  std::lock_guard<std::mutex> Lock() const;

 private:
  struct MockTeapotImpl;
  std::unique_ptr<MockTeapotImpl> impl_;
};

}  // namespace tea
