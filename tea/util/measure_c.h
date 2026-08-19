#pragma once

#include <inttypes.h>
#include <time.h>

#ifdef __APPLE__
#include "mach/mach_time.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

inline int64_t MeasureTicks() {
#ifdef __APPLE__
  return mach_absolute_time();
#elif defined(__x86_64__) || defined(__amd64__)
  uint64_t low, high;
  __asm__ volatile("rdtsc" : "=a"(low), "=d"(high));
  return ((high << 32) | low);
#elif defined(__aarch64__)
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
#else
  static_assert(false, "MeasureTicks not implemented for this architecture");
#endif
}

#ifdef __cplusplus
}
#endif