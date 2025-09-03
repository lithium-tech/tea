#pragma once

#include <mutex>
#include <optional>
#include <thread>

#include "tea/util/measure.h"

namespace tea {

class OneThreadMultishotTimer {
 public:
  void Resume();

  void Suspend();

  std::optional<DurationTicks> GetTotalDuration() const;

 private:
  DurationTicks total_duration_{};

  mutable std::mutex mutex_;

  std::optional<int64_t> last_resume_moment_;
  mutable std::optional<std::thread::id> working_thread_id_;

  // it is assumed that class used only by one thread at a time and WaitEnd is called after WaitStart
  bool is_assumption_correct_ = true;
};

}  // namespace tea
