#include "tea/samovar/network_layer/redis_client.h"

#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include "tea/common/config.h"
#include "tea/observability/tea_log.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/util/measure.h"

namespace tea::samovar {

namespace {

timeval ConvertTimeout(std::chrono::milliseconds timeout) {
  int num_seconds = timeout.count() / 1000;
  int num_rest_milliseconds = timeout.count() - 1000 * num_seconds;
  timeval conn_timeout = {num_seconds, num_rest_milliseconds * 1000};
  return conn_timeout;
}

}  // namespace

RedisReply::RedisReply(redisContext* context, const std::vector<std::string>& argv) {
  std::vector<size_t> argvlen;
  argvlen.reserve(argv.size());
  for (const auto& arg : argv) {
    argvlen.push_back(arg.size());
  }

  std::vector<const char*> argv_values;
  argvlen.reserve(argv.size());
  for (const auto& arg : argv) {
    argv_values.push_back(arg.c_str());
  }

  reply_ = std::shared_ptr<redisReply>(
      static_cast<redisReply*>(redisCommandArgv(context, argv.size(), argv_values.data(), argvlen.data())),
      freeReplyObject);
}

RedisClient::RedisClient(const std::vector<Endpoint>& endpoints, std::chrono::milliseconds request_timeout,
                         std::chrono::milliseconds connection_timeout)
    : redis_context_(nullptr, redisFree), request_timeout_(request_timeout), connection_timeout_(connection_timeout) {
  std::optional<Endpoint> chosen_checkpoint_;
  for (size_t i = 0; i < endpoints.size(); ++i) {
    if (!TryConnect(endpoints[i])) {
      TEA_LOG("Non available redis host " + endpoints[i].host + ":" + std::to_string(endpoints[i].port));
    } else {
      chosen_checkpoint_ = endpoints[i];
      TEA_LOG("Redis to processing - " + endpoints[i].host + ":" + std::to_string(endpoints[i].port));
      break;
    }
  }
  if (!chosen_checkpoint_) {
    throw std::runtime_error("No available server redis");
  }
}

RedisReply RedisClient::SendRequest(const std::vector<std::string>& argv) {
  RedisReply rsp;
  {
    ScopedTimerTicks timer_gandiva_build(sum_time_response_);
    rsp = RedisReply(redis_context_.get(), argv);
  }

  ++requests_count_;

  if (!rsp.Get() || ErrorOnContext()) {
    ++error_count_;
  }
  return rsp;
}

SamovarRedisClient::SamovarRedisClient(const std::vector<Endpoint>& endpoints,
                                       std::chrono::milliseconds request_timeout,
                                       std::chrono::milliseconds connection_timeout)
    : underground_client_(std::make_shared<RedisClient>(endpoints, request_timeout, connection_timeout)) {}

bool RedisClient::TryConnect(const Endpoint& endpoint) {
  timeval conn_timeout = ConvertTimeout(connection_timeout_);
  redis_context_ = std::unique_ptr<redisContext, void (*)(redisContext*)>(
      redisConnectWithTimeout(endpoint.host.c_str(), endpoint.port, conn_timeout), redisFree);
  if (!redis_context_) {
    error_count_++;
    return false;
  }

  timeval req_timeout = ConvertTimeout(request_timeout_);
  if (redisSetTimeout(redis_context_.get(), req_timeout) != REDIS_OK) {
    error_count_++;
    return false;
  }

  auto ping_reply = SendRequest({"PING"}).Get();
  if (!ping_reply || ping_reply->type == REDIS_REPLY_ERROR || redis_context_->err) {
    redis_context_.reset();
    error_count_++;
    return false;
  }
  return true;
}

bool RedisClient::ErrorOnContext() const { return redis_context_->err; }

std::string RedisClient::GetErrorMessage() const { return redis_context_->errstr; }

DurationTicks RedisClient::GetTotalResponseDurationTicks() const { return sum_time_response_; }

int64_t RedisClient::GetRequestCount() const { return requests_count_; }

int64_t RedisClient::GetErrorCount() const { return error_count_; }

void SamovarRedisClient::PushQueue(const std::string& queue_name, const std::vector<std::string>& elements) {
  std::vector<std::string> args{"LPUSH", queue_name};
  args.insert(args.end(), elements.begin(), elements.end());
  auto reply = underground_client_->SendRequest(args);
  auto reply_repr = reply.Get();
  if (ErrorOnMessage(reply_repr)) {
    throw std::runtime_error("Redis cluster is unavailable " + underground_client_->GetErrorMessage());
  }
}

std::vector<std::string> SamovarRedisClient::PopQueue(const std::string& queue_name, int num_elements) {
  std::vector<std::string> argv = {"RPOP", queue_name};
  if (num_elements != 1) {
    argv.push_back(std::to_string(num_elements));
  }

  auto reply = underground_client_->SendRequest(argv);
  auto reply_repr = reply.Get();

  if (ErrorOnMessage(reply_repr)) {
    throw std::runtime_error("Can not pop from queue " + queue_name + ": " + underground_client_->GetErrorMessage());
  }

  std::vector<std::string> result;
  if (reply_repr->type == REDIS_REPLY_ARRAY) {
    for (size_t i = 0; i < reply_repr->elements; ++i) {
      if (!reply_repr->element[i]) {
        continue;
      }
      result.emplace_back(std::string(reply_repr->element[i]->str, reply_repr->element[i]->len));
    }
  } else if (reply_repr->type == REDIS_REPLY_STRING) {
    result.emplace_back(std::string(reply_repr->str, reply_repr->len));
  } else if (reply_repr->type != REDIS_REPLY_NIL) {
    throw std::runtime_error("Unexpected result type in response " + std::to_string(reply_repr->type) + " " +
                             underground_client_->GetErrorMessage());
  }

  return result;
}

void SamovarRedisClient::SetCell(const std::string& cell_name, const std::string& message, std::chrono::seconds ttl) {
  auto reply = underground_client_->SendRequest({"SET", cell_name, message, "EX", std::to_string(ttl.count())});
  auto reply_repr = reply.Get();
  if (ErrorOnMessage(reply_repr)) {
    throw std::runtime_error("Can not set cell " + cell_name + ": " + underground_client_->GetErrorMessage());
  }
}

std::optional<std::string> SamovarRedisClient::GetCell(const std::string& cell_name) {
  auto reply = underground_client_->SendRequest({"GET", cell_name});
  auto reply_repr = reply.Get();
  if (ErrorOnMessage(reply_repr)) {
    throw std::runtime_error("Can not get cell " + cell_name + ": " + underground_client_->GetErrorMessage());
  }
  if (reply_repr->type == REDIS_REPLY_NIL) {
    return std::nullopt;
  }
  return std::string(reply_repr->str, reply_repr->len);
}

void SamovarRedisClient::SetNumericCell(const std::string& cell_name, int value, std::chrono::seconds ttl) {
  std::string message = std::to_string(value);
  SetCell(cell_name, message, ttl);
}

std::optional<int> SamovarRedisClient::GetNumericCell(const std::string& cell_name) {
  auto reply = underground_client_->SendRequest({"GET", cell_name});
  auto reply_repr = reply.Get();
  if (ErrorOnMessage(reply_repr)) {
    throw std::runtime_error("Can not get numeric cell " + cell_name + ": " + underground_client_->GetErrorMessage());
  }
  if (reply_repr->type == REDIS_REPLY_INTEGER) {
    return reply_repr->integer;
  } else if (reply_repr->type == REDIS_REPLY_STRING) {
    return std::stoi(reply_repr->str);
  } else if (reply_repr->type == REDIS_REPLY_NIL) {
    return std::nullopt;
  } else {
    throw std::runtime_error("Can not get numeric cell " + cell_name + ": unexpected response type");
  }
}

int SamovarRedisClient::IncreaseNumericCell(const std::string& cell_name) {
  auto reply = underground_client_->SendRequest({"INCR", cell_name});
  auto reply_repr = reply.Get();
  if (ErrorOnMessage(reply_repr)) {
    throw std::runtime_error("Can not increase numeric cell " + cell_name + ": " +
                             underground_client_->GetErrorMessage());
  }
  return reply_repr->integer;
}

int SamovarRedisClient::DecreaseNumericCell(const std::string& cell_name) {
  auto reply = underground_client_->SendRequest({"DECR", cell_name});
  auto reply_repr = reply.Get();
  if (ErrorOnMessage(reply_repr)) {
    throw std::runtime_error("Can not decrease numeric cell " + cell_name + ": " +
                             underground_client_->GetErrorMessage());
  }
  return reply_repr->integer;
}

void SamovarRedisClient::UpdateTTL(const std::string& object, std::chrono::seconds ttl) {
  auto ttl_str = std::to_string(ttl.count());
  auto reply = underground_client_->SendRequest({"EXPIRE", object, ttl_str}).Get();
  if (ErrorOnMessage(reply)) {
    throw std::runtime_error("Can not update TTL on onject " + object + ": " + underground_client_->GetErrorMessage());
  }
}

void SamovarRedisClient::DeleteCell(const std::string& object) { underground_client_->SendRequest({"DEL", object}); }

bool SamovarRedisClient::ErrorOnMessage(std::shared_ptr<redisReply> reply) const {
  return !reply || underground_client_->ErrorOnContext();
}

DurationTicks SamovarRedisClient::GetTotalResponseDurationTicks() const {
  return underground_client_->GetTotalResponseDurationTicks();
}

int64_t SamovarRedisClient::GetRequestCount() const { return underground_client_->GetRequestCount(); }

int64_t SamovarRedisClient::GetErrorsCount() const { return underground_client_->GetErrorCount(); }

std::vector<std::string> SamovarRedisClient::GetAllKeys() {
  auto reply = underground_client_->SendRequest({"KEYS", "*"});
  auto reply_repr = reply.Get();
  if (ErrorOnMessage(reply_repr)) {
    throw std::runtime_error("Can not get all keys from redis");
  }
  std::vector<std::string> result;
  if (reply_repr->type == REDIS_REPLY_ARRAY) {
    for (size_t i = 0; i < reply_repr->elements; ++i) {
      if (!reply_repr->element[i]) {
        continue;
      }
      result.emplace_back(std::string(reply_repr->element[i]->str, reply_repr->element[i]->len));
    }
    return result;
  }
  throw std::runtime_error("Can not get all keys from redis: answer is not array");
}

size_t SamovarRedisClient::GetQueueLen(const std::string& queue_id) {
  auto reply = underground_client_->SendRequest({"LLEN", queue_id}).Get();

  return reply->integer;
}

void SamovarRedisClient::Clear() { auto reply = underground_client_->SendRequest({"FLUSHALL"}).Get(); }

void SamovarRedisClient::AddIntoSet(const std::string& set_key, const std::string& value) {
  auto reply = underground_client_->SendRequest({"SADD", set_key, value}).Get();
  if (ErrorOnMessage(reply)) {
    throw std::runtime_error("Can not add into set");
  }
}

void SamovarRedisClient::RemoveFromSet(const std::string& set_key, const std::string& value) {
  auto reply = underground_client_->SendRequest({"SREM", set_key, value}).Get();
  if (ErrorOnMessage(reply)) {
    throw std::runtime_error("Can not remove from set");
  }
}

bool SamovarRedisClient::ContainsInSet(const std::string& set_key, const std::string& value) {
  auto reply = underground_client_->SendRequest({"SISMEMBER", set_key, value}).Get();
  if (ErrorOnMessage(reply)) {
    throw std::runtime_error("Can not check if contains in set");
  }
  return reply->integer;
}

}  // namespace tea::samovar
