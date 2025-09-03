#pragma once

#include <functional>
#include <utility>

namespace tea {

class Defer {
 public:
  explicit Defer(std::function<void()> callback) : callback_(std::move(callback)) {}

  ~Defer() {
    try {
      callback_();
    } catch (...) {
    }
  }

 private:
  std::function<void()> callback_;
};

}  // namespace tea
