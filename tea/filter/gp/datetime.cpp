#include "tea/filter/gp/datetime.h"

#include <cassert>

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "utils/datetime.h"
#include "utils/timestamp.h"
}

constexpr int64_t kMinTimestamp = -211813488000000000;
constexpr int64_t kMaxTimestamp = 9223371331200000000;

inline bool IsValidTimestamp(int64_t t) { return kMinTimestamp <= t && t < kMaxTimestamp; }

/*
https://github.com/postgres/postgres/blob/89be0b89ae60c63856fd26d82a104781540e2312/src/backend/utils/adt/timestamp.c#L2051
*/
static inline Timestamp dt2local(Timestamp dt, int timezone_offset_seconds) {
  return dt - timezone_offset_seconds * USECS_PER_SEC;
}

/*
https://github.com/postgres/postgres/blob/89be0b89ae60c63856fd26d82a104781540e2312/src/backend/utils/adt/timestamp.c#L5561
*/
int64_t Timestamp2Timestamptz(int64_t timestamp, bool* overflow) {
  assert(overflow);
  *overflow = false;

  TimestampTz result;
  struct pg_tm tt, *tm = &tt;
  fsec_t fsec;
  int tz;

  if (TIMESTAMP_NOT_FINITE(timestamp)) {
    return timestamp;
  }
  /* We don't expect this to fail, but check it pro forma */
  if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) == 0) {
    tz = DetermineTimeZoneOffset(tm, session_timezone);

    result = dt2local(timestamp, -tz);

    if (!IsValidTimestamp(result)) {
      *overflow = true;
      TIMESTAMP_NOBEGIN(result);
      return result;
    }
    return result;
  }
  *overflow = true;
  TIMESTAMP_NOBEGIN(result);
  return result;
}
