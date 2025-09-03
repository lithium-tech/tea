#include <stdlib.h>

#include "postgres.h"  // NOLINT build/include_subdir

#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"

bool errstart(int elevel, const char *filename, int lineno, const char *funcname, const char *domain) { return TRUE; }
void errfinish(int dummy, ...) {}
int errcode(int sqlerrcode) { return 0; }
int errmsg(const char *fmt, ...) { return 0; }
void elog_start(const char *, int, const char *) {}
void elog_finish(int elevel, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
  if (elevel >= ERROR) {
    abort();
  }
}
