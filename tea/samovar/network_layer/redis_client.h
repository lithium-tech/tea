#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "hiredis.h"  // NOLINT

#include "tea/common/config.h"
#include "tea/samovar/network_layer/backoff.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/util/measure.h"

namespace tea::samovar {

static constexpr int kDefaultPort = 6379;

class RedisReply {
 public:
  RedisReply() = default;
  explicit RedisReply(redisContext* context, const std::vector<std::string>& argv);

  std::shared_ptr<redisReply> Get() const { return reply_; }

  explicit RedisReply(redisReply* reply) : reply_(std::move(reply)) {}

 private:
  std::shared_ptr<redisReply> reply_;
};

class RedisClient {
 public:
  explicit RedisClient(const std::vector<Endpoint>& endpoints, std::chrono::milliseconds request_timeout,
                       std::chrono::milliseconds connection_timeout);

  RedisReply SendRequest(const std::vector<std::string>& argv);

  bool ErrorOnContext() const;

  std::string GetErrorMessage() const;
  DurationTicks GetTotalResponseDurationTicks() const;
  int64_t GetRequestCount() const;
  int64_t GetErrorCount() const;

 protected:
  bool TryConnect(const Endpoint& endpoint);

  std::unique_ptr<redisContext, void (*)(redisContext*)> redis_context_;

  DurationTicks sum_time_response_ = 0;
  int64_t requests_count_ = 0;
  int64_t error_count_ = 0;
  std::chrono::milliseconds request_timeout_;
  std::chrono::milliseconds connection_timeout_;
};

class SamovarRedisClient : public ISamovarClient {
 public:
  explicit SamovarRedisClient(const std::vector<Endpoint>& endpoints, std::shared_ptr<IBackoff> backoff,
                              std::chrono::milliseconds request_timeout, std::chrono::milliseconds connection_timeout);

  void PushQueue(const std::string& queue_name, const std::string& message) override;
  std::vector<std::string> PopQueue(const std::string& queue_name, int num_elements) override;

  void SetCell(const std::string& cell_name, const std::string& message, std::chrono::seconds ttl) override;
  std::optional<std::string> GetCell(const std::string& cell_name) override;

  void SetNumericCell(const std::string& cell_name, int value, std::chrono::seconds ttl) override;
  std::optional<int> GetNumericCell(const std::string& cell_name) override;
  int DecreaseNumericCell(const std::string& cell_name) override;
  int IncreaseNumericCell(const std::string& cell_name) override;

  void UpdateTTL(const std::string& object, std::chrono::seconds ttl) override;
  void DeleteCell(const std::string& object) override;

  DurationTicks GetTotalResponseDurationTicks() const override;
  int64_t GetRequestCount() const override;
  int64_t GetErrorsCount() const override;

  void AddIntoSet(const std::string& set_key, const std::string& value) override;
  void RemoveFromSet(const std::string& set_key, const std::string& value) override;
  bool ContainsInSet(const std::string& set_key, const std::string& value) override;

  /// Note: methods below are useful only for tests.
  std::vector<std::string> GetAllKeys();
  size_t GetQueueLen(const std::string& queue_id);
  void Clear();

 private:
  bool ErrorOnMessage(std::shared_ptr<redisReply> reply) const;

  std::shared_ptr<RedisClient> underground_client_;
};

}  // namespace tea::samovar
