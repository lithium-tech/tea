#pragma once

#include <inttypes.h>

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
#else
  static_assert(false);
#endif
}

#ifdef __cplusplus
}
#endif
