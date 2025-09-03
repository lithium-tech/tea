#pragma once

#include <cstdint>

namespace tea {

struct S3Stats {
  int64_t bytes_read = 0;
  int64_t requests = 0;
  int64_t retry_count = 0;
};

}  // namespace tea
