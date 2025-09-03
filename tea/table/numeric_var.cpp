#include "tea/table/numeric_var.h"

namespace tea {
namespace {

template <typename SignedType, typename UnsignedType, int MaxDigits>
void ToNumericVar(SignedType val, int scale, NumericVar *var) {
  init_var(var, (MaxDigits) / DEC_DIGITS);
  UnsignedType uval;
  if (val < 0) {
    var->sign = TEA_NUMERIC_NEG;
    uval = -val;
  } else {
    var->sign = TEA_NUMERIC_POS;
    uval = val;
  }
  var->dscale = scale;
  if (val == 0) {
    var->ndigits = 0;
    var->weight = 0;
    return;
  }
  NumericDigit *ptr = var->digits + var->ndigits;
  int rem_ddigits = scale % DEC_DIGITS;
  int ndigits = 0;
  int weight = -scale / DEC_DIGITS - 1;
  UnsignedType newuval = 0;
  if (rem_ddigits) {
    ptr--;
    ndigits++;
    switch (rem_ddigits) {
      case 1:
        newuval = uval / 10;
        *ptr = (uval - newuval * 10) * 1000;
        break;
      case 2:
        newuval = uval / 100;
        *ptr = (uval - newuval * 100) * 100;
        break;
      case 3:
        newuval = uval / 1000;
        *ptr = (uval - newuval * 1000) * 10;
        break;
    }
    uval = newuval;
  }

  while (uval) {
    ptr--;
    ndigits++;
    newuval = uval / NBASE;
    *ptr = uval - newuval * NBASE;
    uval = newuval;
    ++weight;
  }
  var->digits = ptr;
  var->ndigits = ndigits;
  var->weight = weight;
}

template <typename SignedType>
bool FromNumericVar(const NumericVar *var, int scale, SignedType *result) {
  int trailing_zero_digits;
  int rem_ddigits = scale % DEC_DIGITS;
  int full_digits = var->weight + scale / DEC_DIGITS + 1;
  if (full_digits > var->ndigits) {
    trailing_zero_digits = full_digits - var->ndigits;
    full_digits = var->ndigits;
  } else {
    trailing_zero_digits = 0;
  }
  SignedType val = 0;
  int i;
  for (i = 0; i < full_digits; i++) {
    val *= NBASE;
    val += var->digits[i];
  }
  if (trailing_zero_digits || i >= var->ndigits) {
    for (i = 0; i < trailing_zero_digits; i++) {
      val *= NBASE;
    }
    switch (rem_ddigits) {
      case 0:
        break;
      case 1:
        val *= 10;
        break;
      case 2:
        val *= 100;
        break;
      case 3:
        val *= 1000;
        break;
    }
  } else {
    switch (rem_ddigits) {
      case 0:
        break;
      case 1:
        val *= 10;
        val += var->digits[i] / 1000;
        break;
      case 2:
        val *= 100;
        val += var->digits[i] / 100;
        break;
      case 3:
        val *= 1000;
        val += var->digits[i] / 10;
        break;
    }
  }

  *result = (var->sign == TEA_NUMERIC_NEG) ? -val : val;
  return true;
}

}  // namespace

void Int64ToNumericVar(int64_t val, int scale, NumericVar *var) {
  ToNumericVar<int64_t, uint64_t, 20>(val, scale, var);
}

void Int128ToNumericVar(Int128 val, int scale, NumericVar *var) { ToNumericVar<Int128, UInt128, 40>(val, scale, var); }

int NumericVarToInt64(NumericVar *var, int scale, int64_t *val) { return FromNumericVar(var, scale, val); }

int NumericVarToInt128(NumericVar *var, int scale, Int128 *val) { return FromNumericVar(var, scale, val); }
}  // namespace tea
