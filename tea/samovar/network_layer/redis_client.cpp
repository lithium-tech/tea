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

std::vector<RedisReply> RedisClient::SendPipeline(const std::vector<std::vector<std::string>>& pipeline_argv) {
  if (pipeline_argv.empty()) {
    return {};
  }

  for (const auto& argv : pipeline_argv) {
    std::vector<size_t> argvlen;
    argvlen.reserve(argv.size());
    for (const auto& arg : argv) {
      argvlen.push_back(arg.size());
    }

    std::vector<const char*> argv_values;
    argv_values.reserve(argv.size());
    for (const auto& arg : argv) {
      argv_values.push_back(arg.c_str());
    }

    if (redisAppendCommandArgv(redis_context_.get(), argv.size(), argv_values.data(), argvlen.data()) != REDIS_OK) {
      ++error_count_;
      throw std::runtime_error("Can not append command to redis pipeline: " + GetErrorMessage());
    }
  }

  std::vector<RedisReply> replies;
  replies.reserve(pipeline_argv.size());
  {
    ScopedTimerTicks timer(sum_time_response_);
    for (size_t i = 0; i < pipeline_argv.size(); ++i) {
      void* reply = nullptr;
      if (redisGetReply(redis_context_.get(), &reply) != REDIS_OK) {
        ++error_count_;
        throw std::runtime_error("Can not get reply from redis pipeline: " + GetErrorMessage());
      }
      replies.emplace_back(static_cast<redisReply*>(reply));
    }
  }

  ++requests_count_;

  for (const auto& rsp : replies) {
    if (!rsp.Get() || ErrorOnContext()) {
      ++error_count_;
      break;
    }
  }

  return replies;
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

int64_t SamovarRedisClient::IncreaseNumericCellBy(const std::string& cell_name, int64_t amount) {
  auto reply = underground_client_->SendRequest({"INCRBY", cell_name, std::to_string(amount)});
  auto reply_repr = reply.Get();
  if (ErrorOnMessage(reply_repr)) {
    throw std::runtime_error("Can not increase numeric cell " + cell_name + " by " + std::to_string(amount) + ": " +
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
  std::chrono::seconds current_ts =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
  if (object_to_last_update_in_seconds_.contains(object)) {
    const auto& last_update = object_to_last_update_in_seconds_.at(object);
    if (2 * (current_ts - last_update) < ttl) {
      return;
    }
  }

  object_to_last_update_in_seconds_[object] = current_ts;

  auto ttl_str = std::to_string(ttl.count());
  auto reply = underground_client_->SendRequest({"EXPIRE", object, ttl_str}).Get();
  if (ErrorOnMessage(reply)) {
    throw std::runtime_error("Can not update TTL on onject " + object + ": " + underground_client_->GetErrorMessage());
  }
}

void SamovarRedisClient::DeleteCell(const std::string& object) { underground_client_->SendRequest({"DEL", object}); }

int SamovarRedisClient::RegisterSegment(const std::string& query_scans_count_key,
                                        const std::vector<std::string>& cells_to_register, std::chrono::seconds ttl,
                                        bool check_query_scans) {
  std::vector<std::vector<std::string>> pipeline;
  std::string ttl_str = std::to_string(ttl.count());

  if (check_query_scans) {
    pipeline.push_back({"INCR", query_scans_count_key});
    pipeline.push_back({"EXPIRE", query_scans_count_key, ttl_str});
  }
  for (const auto& cell : cells_to_register) {
    pipeline.push_back({"INCR", cell});
    pipeline.push_back({"EXPIRE", cell, ttl_str});
  }

  if (pipeline.empty()) {
    return 0;
  }

  auto replies = underground_client_->SendPipeline(pipeline);
  if (replies.size() != pipeline.size()) {
    throw std::runtime_error("Unexpected replies count from pipeline");
  }

  for (const auto& reply : replies) {
    if (ErrorOnMessage(reply.Get())) {
      throw std::runtime_error("Can not register segment in samovar: " + underground_client_->GetErrorMessage());
    }
  }

  int scans_count = 0;
  if (check_query_scans) {
    auto reply_repr = replies[0].Get();
    if (reply_repr->type == REDIS_REPLY_INTEGER) {
      scans_count = reply_repr->integer;
    } else {
      throw std::runtime_error("Unexpected reply type for INCR query_scans_count");
    }
  }

  std::chrono::seconds current_ts =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
  if (check_query_scans) {
    object_to_last_update_in_seconds_[query_scans_count_key] = current_ts;
  }
  for (const auto& cell : cells_to_register) {
    object_to_last_update_in_seconds_[cell] = current_ts;
  }

  return scans_count;
}

void SamovarRedisClient::PublishData(const std::string& queue_name, const std::vector<std::string>& queue_elements,
                                     const std::vector<std::pair<std::string, std::string>>& cells_with_data,
                                     std::chrono::seconds ttl) {
  std::vector<std::vector<std::string>> pipeline;
  std::string ttl_str = std::to_string(ttl.count());

  if (!queue_name.empty() && !queue_elements.empty()) {
    std::vector<std::string> lpush_cmd = {"LPUSH", queue_name};
    lpush_cmd.insert(lpush_cmd.end(), queue_elements.begin(), queue_elements.end());
    pipeline.push_back(std::move(lpush_cmd));
    pipeline.push_back({"EXPIRE", queue_name, ttl_str});
  }

  for (const auto& [cell, data] : cells_with_data) {
    pipeline.push_back({"SET", cell, data, "EX", ttl_str});
  }

  if (pipeline.empty()) {
    return;
  }

  auto replies = underground_client_->SendPipeline(pipeline);
  if (replies.size() != pipeline.size()) {
    throw std::runtime_error("Unexpected replies count from PublishData pipeline");
  }

  for (const auto& reply : replies) {
    if (ErrorOnMessage(reply.Get())) {
      throw std::runtime_error("Can not publish data in samovar: " + underground_client_->GetErrorMessage());
    }
  }

  std::chrono::seconds current_ts =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
  if (!queue_name.empty() && !queue_elements.empty()) {
    object_to_last_update_in_seconds_[queue_name] = current_ts;
  }
  for (const auto& [cell, _] : cells_with_data) {
    object_to_last_update_in_seconds_[cell] = current_ts;
  }
}

std::vector<RedisReply> SamovarRedisClient::SendPipeline(const std::vector<std::vector<std::string>>& pipeline_argv) {
  return underground_client_->SendPipeline(pipeline_argv);
}

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
