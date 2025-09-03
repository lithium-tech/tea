#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef struct CSerializedFilter {
  char* all_extracted;
  char* row;
} CSerializedFilter;

typedef struct IntSpan {
  const int32_t* data;
  int32_t len;
} IntSpan;

typedef struct IgnoredExprs {
  IntSpan op_exprs;
  IntSpan func_exprs;
} IgnoredExprs;

#ifdef __cplusplus
}
#endif
