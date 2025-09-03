#ifndef TEA_FDW_OPTIONS_H_
#define TEA_FDW_OPTIONS_H_

#include "postgres.h"

#include "nodes/pg_list.h"

char* TeaGetLocation(Oid foreigntableid);

#endif  // TEA_FDW_OPTIONS_H_
