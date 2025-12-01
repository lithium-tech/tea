#include "tea/table/numeric.h"

#include "postgres.h"  // NOLINT build/include_subdir

#include "tea/table/numeric_var.h"

/*********************************************************************
 * This is copy-pasted from Postgres src/backend/utils/adt/numeric.c *
 *********************************************************************/
#if GP_VERSION_NUM >= 60000

struct NumericShort {
  uint16 n_header;        /* Sign + display scale + weight */
  NumericDigit n_data[1]; /* Digits */
};

struct NumericLong {
  uint16 n_sign_dscale;   /* Sign + display scale */
  int16 n_weight;         /* Weight of 1st digit */
  NumericDigit n_data[1]; /* Digits */
};

union NumericChoice {
  uint16 n_header;             /* Header word */
  struct NumericLong n_long;   /* Long form (4-byte header) */
  struct NumericShort n_short; /* Short form (2-byte header) */
};

struct NumericData {
  int32 vl_len_;              /* varlena header (do not touch directly!) */
  union NumericChoice choice; /* choice of format */
};

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_SHORT 0x8000
#define NUMERIC_NAN 0xC000
#define NUMERIC_NEG TEA_NUMERIC_NEG
#define NUMERIC_POS TEA_NUMERIC_POS

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_NAN(n) (NUMERIC_FLAGBITS(n) == NUMERIC_NAN)
#define NUMERIC_IS_SHORT(n) (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)

#define NUMERIC_HDRSZ (VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

#define NUMERIC_HEADER_IS_SHORT(n) (((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) (VARHDRSZ + sizeof(uint16) + (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

#define NUMERIC_SHORT_SIGN_MASK 0x2000
#define NUMERIC_SHORT_DSCALE_MASK 0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT 7
#define NUMERIC_SHORT_DSCALE_MAX (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK 0x003F
#define NUMERIC_SHORT_WEIGHT_MAX NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN (-(NUMERIC_SHORT_WEIGHT_MASK + 1))

#define NUMERIC_DSCALE_MASK 0x3FFF

#define NUMERIC_SIGN(n)                                                                                         \
  (NUMERIC_IS_SHORT(n) ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? NUMERIC_NEG : NUMERIC_POS) \
                       : NUMERIC_FLAGBITS(n))
#define NUMERIC_DSCALE(n)                                                                         \
  (NUMERIC_HEADER_IS_SHORT((n))                                                                   \
       ? ((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT \
       : ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n)                                                                                    \
  (NUMERIC_HEADER_IS_SHORT((n))                                                                              \
       ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) | \
          ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK))                                        \
       : ((n)->choice.n_long.n_weight))

#define NUMERIC_DIGITS(num) (NUMERIC_HEADER_IS_SHORT(num) ? (num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) ((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))

NumericData* tea::NumericVarToNumeric(NumericVar* var) {
  NumericData* result;
  NumericDigit* digits = var->digits;
  int weight = var->weight;
  int sign = var->sign;
  int n;
  Size len;
  bool can_be_short;

  if (sign == NUMERIC_NAN) {
    result = (NumericData*)palloc(NUMERIC_HDRSZ_SHORT);

    SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
    result->choice.n_header = NUMERIC_NAN;
    /* the header word is all we need */
    return result;
  }

  n = var->ndigits;

  /* truncate leading zeroes */
  while (n > 0 && *digits == 0) {
    digits++;
    weight--;
    n--;
  }
  /* truncate trailing zeroes */
  while (n > 0 && digits[n - 1] == 0) n--;

  /* If zero result, force to weight=0 and positive sign */
  if (n == 0) {
    weight = 0;
    sign = NUMERIC_POS;
  }

  can_be_short = var->dscale <= NUMERIC_SHORT_DSCALE_MAX && weight <= NUMERIC_SHORT_WEIGHT_MAX &&
                 weight >= NUMERIC_SHORT_WEIGHT_MIN;
  /* Build the result */
  if (can_be_short) {
    len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
    result = (NumericData*)palloc(len);
    SET_VARSIZE(result, len);
    result->choice.n_short.n_header =
        (sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
        (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT) | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
        (weight & NUMERIC_SHORT_WEIGHT_MASK);
  } else {
    len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
    result = (NumericData*)palloc(len);
    SET_VARSIZE(result, len);
    result->choice.n_long.n_sign_dscale = sign | (var->dscale & NUMERIC_DSCALE_MASK);
    result->choice.n_long.n_weight = weight;
  }

  memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));
  Assert(NUMERIC_NDIGITS(result) == n);

  /* Check for overflow of int16 fields */
  if (NUMERIC_WEIGHT(result) != weight || NUMERIC_DSCALE(result) != var->dscale)
    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));

  return result;
}

#else /* GP < 6.0 */

#include "utils/numeric.h"

#define NUMERIC_WEIGHT(num) ((num)->n_weight)
#define NUMERIC_DIGITS(num) ((NumericDigit*)(num)->n_data)
#define NUMERIC_NDIGITS(num) ((VARSIZE(num) - NUMERIC_HDRSZ) / sizeof(NumericDigit))

NumericData* tea::NumericVarToNumeric(NumericVar* var) {
  NumericData* result;
  NumericDigit* digits = var->digits;
  int weight = var->weight;
  int sign = var->sign;
  int n;
  Size len;

  if (sign == NUMERIC_NAN) {
    result = (NumericData*)palloc(NUMERIC_HDRSZ);

    SET_VARSIZE(result, NUMERIC_HDRSZ);
    result->n_weight = 0;
    result->n_sign_dscale = NUMERIC_NAN;

    return result;
  }

  n = var->ndigits;

  /* truncate leading zeroes */
  while (n > 0 && *digits == 0) {
    digits++;
    weight--;
    n--;
  }
  /* truncate trailing zeroes */
  while (n > 0 && digits[n - 1] == 0) n--;

  /* If zero result, force to weight=0 and positive sign */
  if (n == 0) {
    weight = 0;
    sign = NUMERIC_POS;
  }

  /* Build the result */
  len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
  result = (NumericData*)palloc(len);
  SET_VARSIZE(result, len);
  result->n_weight = weight;
  result->n_sign_dscale = sign | (var->dscale & NUMERIC_DSCALE_MASK);

  memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));

  if (NUMERIC_WEIGHT(result) != weight || NUMERIC_DSCALE(result) != var->dscale)
    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));

  return result;
}

/*********************************************************************
 * End of the snippet of src/backend/utils/adt/numeric.c             *
 *********************************************************************/
#endif /* GP_VERSION_NUM >= 60000 */
