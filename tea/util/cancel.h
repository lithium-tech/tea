#pragma once

#include <atomic>

namespace tea {

class CancelToken {
 public:
  void Cancel() { state_.store(true); }

  bool IsCancelled() const { return state_.load(); }

 private:
  std::atomic<bool> state_{};
};

}  // namespace tea
