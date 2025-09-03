#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "tea/common/config.h"

namespace tea::samovar {

struct IBackoff {
  virtual void Wait() = 0;
  virtual void OnSuccess() = 0;

  virtual ~IBackoff() = default;
};

struct IContextLogger {
  virtual std::string GetLog() const = 0;

  virtual ~IContextLogger() = default;
};

class NoBackoff : public IBackoff {
 public:
  explicit NoBackoff(unsigned int limit_retries, std::shared_ptr<IContextLogger> logger);

  void Wait() override;

  void OnSuccess() override { num_iterations_ = 0; }

 private:
  unsigned int limit_retries_;

  unsigned int num_iterations_ = 0;
  std::shared_ptr<IContextLogger> logger_;
};

class LinearBackoff : public IBackoff {
 public:
  // Sleep time in milliseconds
  explicit LinearBackoff(unsigned int limit_retries, std::chrono::milliseconds duration,
                         std::shared_ptr<IContextLogger> logger);

  void Wait() override;

  void OnSuccess() override { num_iterations_ = 0; }

 private:
  unsigned int limit_retries_;
  std::chrono::milliseconds duration_;

  unsigned int num_iterations_ = 0;
  std::shared_ptr<IContextLogger> logger_;
};

class ExponentialBackoff : public IBackoff {
 public:
  ExponentialBackoff(unsigned int limit_retries, double sleep_coef,
                     std::optional<std::chrono::milliseconds> waiting_limit, std::shared_ptr<IContextLogger> logger);

  void Wait() override;

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
  std::shared_ptr<IContextLogger> logger_;
};

std::shared_ptr<IBackoff> CreateBackoff(const SamovarConfig& config, std::shared_ptr<IContextLogger> logger);

}  // namespace tea::samovar
