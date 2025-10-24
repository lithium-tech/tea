#include "tea/samovar/network_layer/backoff.h"

#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <memory>
#include <stdexcept>
#include <thread>

#include "tea/observability/tea_log.h"
#include "tea/util/cancel.h"

namespace tea::samovar {

namespace {

static constexpr int kDefaultIncrement = 2;

}  // namespace

NoBackoff::NoBackoff(unsigned int limit_retries, std::shared_ptr<IContextLogger> logger)
    : limit_retries_(limit_retries), logger_(logger) {}

void NoBackoff::Wait() {
  num_iterations_++;
  if (limit_retries_ <= num_iterations_) {
    auto additional_log = logger_ ? logger_->GetLog() : "";
    throw std::runtime_error("Samovar: Backoff limit exceeded " + additional_log);
  }
}

LinearBackoff::LinearBackoff(unsigned int limit_retries, std::chrono::milliseconds duration,
                             const CancelToken& cancel_token, std::shared_ptr<IContextLogger> logger)
    : limit_retries_(limit_retries), duration_(duration), cancel_token_(cancel_token), logger_(logger) {}

void LinearBackoff::Wait() {
  num_iterations_++;
  if (limit_retries_ <= num_iterations_) {
    auto additional_log = logger_ ? logger_->GetLog() : "";
    throw std::runtime_error("Samovar: Backoff limit exceeded " + additional_log);
  }

  if (cancel_token_.WaitFor(duration_)) {
    auto additional_log = logger_ ? logger_->GetLog() : "";
    throw std::runtime_error("Samovar: query is cancelled " + additional_log);
  }
}

ExponentialBackoff::ExponentialBackoff(unsigned int limit_retries, double sleep_coef,
                                       std::optional<std::chrono::milliseconds> waiting_limit,
                                       const CancelToken& cancel_token, std::shared_ptr<IContextLogger> logger)
    : limit_retries_(limit_retries),
      sleep_coef_(sleep_coef),
      waiting_limit_(waiting_limit),
      cancel_token_(cancel_token),
      logger_(logger) {}

void ExponentialBackoff::Wait() {
  num_iterations_++;
  if (limit_retries_ <= num_iterations_) {
    auto additional_log = logger_ ? logger_->GetLog() : "";
    throw std::runtime_error("Samovar: Backoff limit exceeded " + additional_log);
  }

  current_waiting_time_ms_ = std::min(static_cast<int64_t>(waiting_limit_->count()),
                                      static_cast<int64_t>(current_waiting_time_ms_ * sleep_coef_));
  if (cancel_token_.WaitFor(std::chrono::milliseconds(current_waiting_time_ms_))) {
    auto additional_log = logger_ ? logger_->GetLog() : "";
    throw std::runtime_error("Samovar: query is cancelled " + additional_log);
  }
}

std::shared_ptr<IBackoff> CreateBackoff(const SamovarConfig& config, const CancelToken& cancel_token,
                                        std::shared_ptr<IContextLogger> logger) {
  std::shared_ptr<IBackoff> backoff;
  switch (config.backoff_type) {
    case BackoffType::kNoBackoff: {
      TEA_LOG("Do not use backoff");
      backoff = std::make_shared<NoBackoff>(config.limit_retries, logger);
      break;
    }
    case BackoffType::kLinearBackoff: {
      TEA_LOG("Use linear backoff");
      backoff = std::make_shared<LinearBackoff>(config.limit_retries, config.linear_backoff_time_to_sleep_ms,
                                                cancel_token, logger);
      break;
    }
    case BackoffType::kExponentialBackoff: {
      TEA_LOG("Use exp backoff");
      auto limit_backoff = config.exponential_backoff_limit.value_or(std::chrono::milliseconds(10000));
      backoff = std::make_shared<ExponentialBackoff>(config.limit_retries,
                                                     config.exponential_backoff_sleep_coef.value_or(kDefaultIncrement),
                                                     limit_backoff, cancel_token, logger);
      break;
    }
  }

  return backoff;
}

}  // namespace tea::samovar
