#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "tea/common/config.h"
#include "tea/util/cancel.h"

namespace tea::samovar {

struct IBackoff {
  enum class Result { kQueryCancelled, kShouldRetry, kStop };

  [[nodiscard]] virtual Result Wait() = 0;
  virtual void OnSuccess() = 0;

  virtual ~IBackoff() = default;
};

class NoBackoff : public IBackoff {
 public:
  explicit NoBackoff(unsigned int limit_retries);

  [[nodiscard]] Result Wait() override;

  void OnSuccess() override { num_iterations_ = 0; }

 private:
  unsigned int limit_retries_;

  unsigned int num_iterations_ = 0;
};

class LinearBackoff : public IBackoff {
 public:
  // Sleep time in milliseconds
  explicit LinearBackoff(unsigned int limit_retries, std::chrono::milliseconds duration,
                         const CancelToken& cancel_token);

  [[nodiscard]] Result Wait() override;

  void OnSuccess() override { num_iterations_ = 0; }

 private:
  unsigned int limit_retries_;
  std::chrono::milliseconds duration_;

  unsigned int num_iterations_ = 0;
  const CancelToken& cancel_token_;
};

class ExponentialBackoff : public IBackoff {
 public:
  ExponentialBackoff(unsigned int limit_retries, double sleep_coef,
                     std::optional<std::chrono::milliseconds> waiting_limit, const CancelToken& cancel_token);

  [[nodiscard]] Result Wait() override;

  void OnSuccess() override {
    num_iterations_ = 0;
    current_waiting_time_ms_ = 1;
  }

 private:
  unsigned int limit_retries_;
  unsigned int num_iterations_ = 0;
  int64_t current_waiting_time_ms_ = 1;

  double sleep_coef_;
  std::optional<std::chrono::milliseconds> waiting_limit_;
  const CancelToken& cancel_token_;
};

std::shared_ptr<IBackoff> CreateBackoff(const SamovarConfig& config, const CancelToken& cancel_token);

}  // namespace tea::samovar
