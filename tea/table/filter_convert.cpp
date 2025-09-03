#include "tea/table/filter_convert.h"

#include "tea/filter/gp/datetime.h"

namespace tea {

int64_t TimestampToTimestamptzShiftUs() {
  bool overflow;
  return Timestamp2Timestamptz(0, &overflow);
}

}  // namespace tea
