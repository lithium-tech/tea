#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace tea {

class CancelToken {
 public:
  void Cancel() {
    std::lock_guard lg(lock_);
    state_ = State::kCancelled;
    cancel_condition_.notify_all();
  }

  bool IsCancelled() const {
    std::lock_guard lg(lock_);
    return state_ == State::kCancelled;
  }

  template <typename Rep, typename Period>
  bool WaitFor(std::chrono::duration<Rep, Period> duration) const {
    std::unique_lock lg(lock_);

    cancel_condition_.wait_for(lg, duration, [&]() { return state_ == State::kCancelled; });

    return state_ == State::kCancelled;
  }

 private:
  enum class State { kRunning, kCancelled };

  mutable std::mutex lock_;
  mutable std::condition_variable cancel_condition_;
  State state_{State::kRunning};
};

}  // namespace tea
