#pragma once

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ExtStats {
  int64_t heap_form_tuple_ticks;
  int64_t convert_duration_ticks;

  bool is_logged;
} ExtStats;

#ifdef __cplusplus
}
#endif
