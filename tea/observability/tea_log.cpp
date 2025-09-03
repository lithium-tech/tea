#include "tea/observability/tea_log.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir
}

namespace tea {
namespace {

class GreenplumLogger {
 public:
  GreenplumLogger() {}

  void Log(const std::string& message) {
    std::string msg = message;
    std::lock_guard lg(lock_);
    logs_.emplace_back(std::move(msg));
  }

  std::list<std::string> GetLogs() {
    std::lock_guard lg(lock_);
    return std::move(logs_);
  }

 private:
  std::mutex lock_;
  std::list<std::string> logs_;
};

static GreenplumLogger* logger_ = nullptr;

}  // namespace

void Log(const std::string& file_name, int line_number, const std::string& message) {
  std::string message_without_time = message + " (" + file_name + ":" + std::to_string(line_number) + ")";

  auto now = std::chrono::system_clock::now();
  auto now_t = std::chrono::system_clock::to_time_t(now);
  auto localtime_ptr = std::localtime(&now_t);
  if (!localtime_ptr) {
    Log(message_without_time);
    return;
  }

  std::tm tm = *localtime_ptr;
  auto now_ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0') << std::setw(3) << now_ms.count();

  Log(message_without_time + " " + oss.str());
}

void Log(const std::string& message) {
  if (logger_) {
    logger_->Log(message);
  }
}

std::list<std::string> GetLogs() {
  if (logger_) {
    return logger_->GetLogs();
  }
  return std::list<std::string>();
}

void InitializeLogger() {
  if (!logger_) {
    logger_ = new GreenplumLogger();
  }
}

void FinalizeLogger() {
  if (logger_) {
    delete logger_;
    logger_ = nullptr;
  }
}

}  // namespace tea
