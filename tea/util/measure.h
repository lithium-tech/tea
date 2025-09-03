#pragma once

#include <chrono>

#include "tea/util/measure_c.h"

namespace tea {

using TimePointClock = std::chrono::time_point<std::chrono::steady_clock>;
using DurationClock = std::chrono::steady_clock::duration;

using TimePointTicks = int64_t;
using DurationTicks = int64_t;

class ScopedTimerTicks {
 public:
  explicit ScopedTimerTicks(DurationTicks& result) : start_(MeasureTicks()), result_(result) {}
  ~ScopedTimerTicks() { result_ += MeasureTicks() - start_; }

 private:
  TimePointTicks start_;
  DurationTicks& result_;
};

class TimerTicks {
 public:
  TimerTicks() : start_(MeasureTicks()) {}
  DurationTicks duration() const { return MeasureTicks() - start_; }

 private:
  TimePointTicks start_;
};

class TimerClock {
 public:
  TimerClock() : start_(std::chrono::steady_clock::now()) {}
  DurationClock duration() const { return std::chrono::steady_clock::now() - start_; }

 private:
  TimePointClock start_;
};

}  // namespace tea
