#pragma once

#include "pg_config.h"     // NOLINT build/include_subdir
#include "postgres_ext.h"  // NOLINT build/include_subdir

struct varlena;
struct FmgrInfo;
typedef PG_INT64_TYPE GpDatum;

#define BYTEAARRAYOID 1001
#define BOOLARRAYOID 1000
#define TIMESTAMPARRAYOID 1115
#define DATEARRAYOID 1182
#define TIMEARRAYOID 1183
#define TIMESTAMPTZARRAYOID 1185
#define NUMERICARRAYOID 1231
#define UUIDARRAYOID 2951
#define JSONARRAYOID 199
