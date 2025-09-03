#include "tea/util/multishot_timer.h"

#include <mutex>
#include <thread>

#include "tea/util/measure.h"
#include "tea/util/measure_c.h"

namespace tea {

void OneThreadMultishotTimer::Resume() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (working_thread_id_.has_value()) {
    is_assumption_correct_ = false;
    return;
  }
  working_thread_id_ = std::this_thread::get_id();
  last_resume_moment_ = MeasureTicks();
}

void OneThreadMultishotTimer::Suspend() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!working_thread_id_.has_value() || working_thread_id_.value() != std::this_thread::get_id()) {
    is_assumption_correct_ = false;
    return;
  }
  total_duration_ += MeasureTicks() - *last_resume_moment_;

  working_thread_id_.reset();
  last_resume_moment_.reset();
}

std::optional<DurationTicks> OneThreadMultishotTimer::GetTotalDuration() const {
  std::lock_guard<std::mutex> guard(mutex_);
  if (working_thread_id_.has_value() || !is_assumption_correct_) {
    return std::nullopt;
  }
  return total_duration_;
}

}  // namespace tea
