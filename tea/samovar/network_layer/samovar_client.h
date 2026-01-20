#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "tea/samovar/network_layer/backoff.h"
#include "tea/util/measure.h"

namespace tea::samovar {

static constexpr int MAX_RETRIES = 1000000;

template <typename T>
T DoWithRetries(std::function<std::optional<T>()> operation, std::shared_ptr<IBackoff> backoff,
                const std::string& message) {
  for (int i = 0; i < MAX_RETRIES; ++i) {
    auto maybe_answer = operation();
    if (!maybe_answer) {
      IBackoff::Result response = backoff->Wait();
      if (response == IBackoff::Result::kShouldRetry) {
        continue;
      }
      std::string reason = response == IBackoff::Result::kQueryCancelled ? "failed" : "cancelled";
      throw std::runtime_error("Samovar: " + message + " (query " + reason + ")");
    }
    backoff->OnSuccess();
    return *maybe_answer;
  }
  throw std::runtime_error("Max retries limit exceeded");
}

class ISamovarClient {
 public:
  virtual void PushQueue(const std::string& queue_name, const std::vector<std::string>& elements) = 0;
  virtual std::vector<std::string> PopQueue(const std::string& queue_name, int num_elements) = 0;

  virtual void SetCell(const std::string& cell_name, const std::string& message, std::chrono::seconds ttl) = 0;
  virtual std::optional<std::string> GetCell(const std::string& cell_name) = 0;

  virtual void SetNumericCell(const std::string& cell_name, int value, std::chrono::seconds ttl) = 0;
  virtual std::optional<int> GetNumericCell(const std::string& cell_name) = 0;
  virtual int DecreaseNumericCell(const std::string& cell_name) = 0;
  virtual int IncreaseNumericCell(const std::string& cell_name) = 0;

  virtual void UpdateTTL(const std::string& object, std::chrono::seconds ttl) = 0;
  virtual void UpdateTTL(const std::vector<std::string>& object, std::chrono::seconds ttl);
  virtual void DeleteCell(const std::string& object) = 0;

  virtual DurationTicks GetTotalResponseDurationTicks() const = 0;
  virtual int64_t GetRequestCount() const = 0;
  virtual int64_t GetErrorsCount() const = 0;

  virtual void AddIntoSet(const std::string& set_key, const std::string& value) = 0;
  virtual void RemoveFromSet(const std::string& set_key, const std::string& value) = 0;
  virtual bool ContainsInSet(const std::string& set_key, const std::string& value) = 0;

  virtual size_t GetQueueLen(const std::string& queue_id) = 0;

  virtual ~ISamovarClient() = default;
};

}  // namespace tea::samovar
