#pragma once

#include "postgres.h"

#include "access/tupdesc.h"

#include "tea/gpext/tea_column.h"

ReaderScanProjection MakeScanProjection(TupleDesc tupdesc, int ncolumns, int* column_attnums, bool* is_remote_only);
