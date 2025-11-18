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

#include "tea/common/config.h"
#include "tea/observability/tea_log.h"
#include "tea/util/cancel.h"

namespace tea::samovar {

namespace {

static constexpr int kDefaultIncrement = 2;

}  // namespace

NoBackoff::NoBackoff(unsigned int limit_retries) : limit_retries_(limit_retries) {}

IBackoff::Result NoBackoff::Wait() {
  num_iterations_++;
  if (limit_retries_ <= num_iterations_) {
    return Result::kStop;
  }
  return Result::kShouldRetry;
}

LinearBackoff::LinearBackoff(unsigned int limit_retries, std::chrono::milliseconds duration,
                             const CancelToken& cancel_token)
    : limit_retries_(limit_retries), duration_(duration), cancel_token_(cancel_token) {}

IBackoff::Result LinearBackoff::Wait() {
  num_iterations_++;
  if (limit_retries_ <= num_iterations_) {
    return Result::kStop;
  }

  if (cancel_token_.WaitFor(duration_)) {
    return Result::kQueryCancelled;
  }

  return Result::kShouldRetry;
}

ExponentialBackoff::ExponentialBackoff(unsigned int limit_retries, double sleep_coef,
                                       std::optional<std::chrono::milliseconds> waiting_limit,
                                       const CancelToken& cancel_token)
    : limit_retries_(limit_retries),
      sleep_coef_(sleep_coef),
      waiting_limit_(waiting_limit),
      cancel_token_(cancel_token) {}

IBackoff::Result ExponentialBackoff::Wait() {
  num_iterations_++;
  if (limit_retries_ <= num_iterations_) {
    return Result::kStop;
  }

  current_waiting_time_ms_ = std::min(static_cast<int64_t>(waiting_limit_->count()),
                                      static_cast<int64_t>(current_waiting_time_ms_ * sleep_coef_));
  if (cancel_token_.WaitFor(std::chrono::milliseconds(current_waiting_time_ms_))) {
    return Result::kQueryCancelled;
  }

  return Result::kShouldRetry;
}

std::shared_ptr<IBackoff> CreateBackoff(const BackoffInfo& config, const CancelToken& cancel_token) {
  std::shared_ptr<IBackoff> backoff;
  switch (config.backoff_type) {
    case BackoffType::kNoBackoff: {
      TEA_LOG("Do not use backoff");
      backoff = std::make_shared<NoBackoff>(config.limit_retries);
      break;
    }
    case BackoffType::kLinearBackoff: {
      TEA_LOG("Use linear backoff");
      backoff =
          std::make_shared<LinearBackoff>(config.limit_retries, config.linear_backoff_time_to_sleep_ms, cancel_token);
      break;
    }
    case BackoffType::kExponentialBackoff: {
      TEA_LOG("Use exp backoff");
      auto limit_backoff = config.exponential_backoff_limit.value_or(std::chrono::milliseconds(10000));
      backoff = std::make_shared<ExponentialBackoff>(config.limit_retries,
                                                     config.exponential_backoff_sleep_coef.value_or(kDefaultIncrement),
                                                     limit_backoff, cancel_token);
      break;
    }
  }

  return backoff;
}

}  // namespace tea::samovar
