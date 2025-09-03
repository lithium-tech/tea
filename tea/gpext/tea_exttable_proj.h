#pragma once

#include "postgres.h"

#include "access/extprotocol.h"

int GetRequiredColumns(ExternalSelectDesc desc, int* columns, int ncolumns,
                       bool ext_table_filter_walker_for_projection);
