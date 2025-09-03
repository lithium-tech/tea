#pragma once

#include <cstdint>

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "utils/date.h"
#include "utils/datetime.h"
}

namespace tea {

constexpr int64_t kPostgresLagMicros = 946684800000000;
constexpr int64_t kPostgresLagDays = 10957;

static inline int32_t DatumGetUnixDate(Datum datum) { return DatumGetDateADT(datum) + kPostgresLagDays; }
static inline int64_t DatumGetUnixTimestampMicros(Datum datum) { return DatumGetTimestamp(datum) + kPostgresLagMicros; }

}  // namespace tea
