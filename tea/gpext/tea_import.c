// clang-format off
#include "postgres.h"
// clang-format on

#include "tea/gpext/tea_import.h"

#include <stdio.h>

#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"

void GetScanSessionId(char* buf, int size) {
  if (getDistributedTransactionIdentifier(buf)) {
    size_t txlen = strlen(buf);
    buf += txlen;
    size -= txlen;
  }
  snprintf(buf, size, "/%d-%d", gp_session_id, gp_command_count);
}
