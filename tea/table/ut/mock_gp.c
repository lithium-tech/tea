#include <stdlib.h>

#include "postgres.h"  // NOLINT build/include_subdir

#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"

#if PG_VERSION_NUM >= 90400

void *palloc(Size size) { return malloc(size); }

void *palloc0(Size size) {
  void *p = malloc(size);
  memset(p, 0, size);
  return p;
}

extern void *repalloc(void *pointer, Size size) { return realloc(pointer, size); }

void pfree(void *pointer) { return free(pointer); }

#else

MemoryContext CurrentMemoryContext;

void *MemoryContextAllocImpl(MemoryContext context, Size size, const char *file, const char *func, int line) {
  return malloc(size);
}

void *MemoryContextAllocZeroImpl(MemoryContext context, Size size, const char *file, const char *func, int line) {
  void *p = malloc(size);
  memset(p, 0, size);
  return p;
}

void *MemoryContextAllocZeroAlignedImpl(MemoryContext context, Size size, const char *file, const char *func,
                                        int line) {
  void *p = malloc(size);
  memset(p, 0, size);
  return p;
}

void *MemoryContextReallocImpl(void *pointer, Size size, const char *file, const char *func, int line) {
  return realloc(pointer, size);
}

void MemoryContextFreeImpl(void *pointer, const char *file, const char *func, int sline) { return free(pointer); }

#endif

text *cstring_to_text(const char *s) { return cstring_to_text_with_len(s, strlen(s)); }

text *cstring_to_text_with_len(const char *s, int len) {
  text *result = (text *)palloc(len + VARHDRSZ);

  SET_VARSIZE(result, len + VARHDRSZ);
  memcpy(VARDATA(result), s, len);

  return result;
}

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

char *pg_custom_to_server(const char *s, int len, int src_encoding, void *cep) { return (char *)s; }
char *pg_server_to_custom(const char *s, int len, int dest_encoding, void *cep) { return (char *)s; }

ArrayType *construct_array(Datum *, int, Oid, int, bool, char) { return NULL; }

ArrayType *construct_empty_array(Oid) { return NULL; }

ArrayType *construct_md_array(Datum *, bool *, int, int *, int *, Oid, int, bool, char) { return NULL; }

Oid FindDefaultConversionProc(int32 for_encoding, int32 to_encoding) { return 0; }

int GetDatabaseEncoding(void) { return PG_UTF8; }

void fmgr_info(Oid functionId, FmgrInfo *finfo) {}

Oid exprType(const Node *expr);

Oid exprType(const Node *expr) { return 0; }

Node *get_leftop(const Expr *clause) { return NULL; }

Node *get_rightop(const Expr *clause) { return NULL; }

void getTypeOutputInfo(Oid type, Oid *typOutput, bool *typIsVarlena) {}

char *OidOutputFunctionCall(Oid functionId, Datum val) { return NULL; }

text *pg_detoast_datum(text *datum) { return NULL; }

char *text_to_cstring(const text *t) { return NULL; }

/* clang-format off */
extern Datum(DirectFunctionCall1)(PGFunction func, Datum arg1) {
  return 0;
}

extern Datum DirectFunctionCall1Coll(PGFunction func, Oid collation,
                                     Datum arg1) {
  return 0;
}
/* clang-format on */

void deconstruct_array(ArrayType *array, Oid elmtype, int elmlen, bool elmbyval, char elmalign, Datum **elemsp,
                       bool **nullsp, int *nelemsp) {}

Datum textout(FunctionCallInfo fcinfo) { return 0; }

int date2isoweek(int year, int mon, int mday) { return 0; }

#if PG_VERSION_NUM >= 90400

int timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm, fsec_t *fsec, const char **tzn, pg_tz *attimezone) {
  return 0;
}

#else

int timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm, fsec_t *fsec, char **tzn, pg_tz *attimezone) { return 0; }

#endif

extern List *lappend(List *list, void *datum) { return NULL; }

#if PG_VERSION_NUM >= 90400

extern char *pstrdup(const char *in) { return NULL; }

#else

extern char *MemoryContextStrdup(MemoryContext context, const char *string) { return NULL; }

#endif

int interval2tm(Interval span, struct pg_tm *tm, fsec_t *fsec) { return 0; }

pg_tz *session_timezone;

int DetermineTimeZoneOffset(struct pg_tm *tm, pg_tz *tzp) { return 0; }
