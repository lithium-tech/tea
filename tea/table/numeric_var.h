#pragma once
/****************************************************************************
 * This is mostly copy-pasted from Postgres src/backend/utils/adt/numeric.c *
 ***************************************************************************/
#include <assert.h>
#include <stdint.h>
#include <tea/util/int128.h>

#define NBASE 10000
#define HALF_NBASE 5000
#define DEC_DIGITS 4       /* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS 2 /* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS 4

typedef int16_t NumericDigit;

/* rename to TEA_ in order to avoid clashes with GP5 headers */
#define TEA_NUMERIC_POS 0x0000
#define TEA_NUMERIC_NEG 0x4000

#define NUMERIC_LOCAL_NDIG 36
#define NUMERIC_LOCAL_NMAX (NUMERIC_LOCAL_NDIG - 2)

typedef struct NumericVar {
  int ndigits;                          /* # of digits in digits[] - can be 0! */
  int weight;                           /* weight of first digit */
  int sign;                             /* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
  int dscale;                           /* display scale */
  NumericDigit* buf;                    /* start of space for digits[] */
  NumericDigit* digits;                 /* base-NBASE digits */
  NumericDigit ndb[NUMERIC_LOCAL_NDIG]; /* local space for digits[] */
} NumericVar;

#define quick_init_var(v) \
  do {                    \
    (v)->buf = (v)->ndb;  \
    (v)->digits = NULL;   \
  } while (0)

#define init_var(v, n)                 \
  do {                                 \
    assert((n) <= NUMERIC_LOCAL_NMAX); \
    (v)->buf = (v)->ndb;               \
    (v)->ndigits = (n);                \
    (v)->buf[0] = 0;                   \
    (v)->digits = (v)->buf + 1;        \
  } while (0)

namespace tea {

void Int64ToNumericVar(int64_t val, int scale, NumericVar* var);
void Int128ToNumericVar(Int128 val, int scale, NumericVar* var);
int NumericVarToInt64(NumericVar* var, int scale, int64_t* val);
int NumericVarToInt128(NumericVar* var, int scale, Int128* val);

}  // namespace tea
