#pragma once

#include <cstdint>

int64_t Timestamp2Timestamptz(int64_t timestamp, bool* overflow);
