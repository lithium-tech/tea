#pragma once

#include <functional>
#include <map>
#include <memory>
#include <utility>

#include "iceberg/common/logger.h"

namespace tea {

class Logger : public iceberg::ILogger {
 public:
  using Handler = std::function<void(const Message& message)>;

  void Log(const Message& message, const MessageType& message_type) {
    auto it = handlers_.find(message_type);
    if (it != handlers_.end()) {
      const auto& handler = it->second;
      handler(message);
    }
  }

  void SetHandler(MessageType message_type, Handler handler) {
    handlers_[std::move(message_type)] = std::move(handler);
  }

 private:
  std::map<MessageType, Handler> handlers_;
};

class ScopedLogger {
 public:
  ScopedLogger(std::shared_ptr<iceberg::ILogger> logger, const iceberg::ILogger::MessageType& start_type,
               const iceberg::ILogger::MessageType& end_type, const iceberg::ILogger::Message& message = "")
      : logger_(logger) {
    if (logger_) {
      logger_->Log(message, start_type);
      end_type_ = end_type;
      message_ = message;
    }
  }

  ~ScopedLogger() {
    if (logger_) {
      logger_->Log(message_, end_type_);
    }
  }

 private:
  std::shared_ptr<iceberg::ILogger> logger_;
  iceberg::ILogger::MessageType end_type_;
  iceberg::ILogger::Message message_;
};

}  // namespace tea
