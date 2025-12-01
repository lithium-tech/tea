#pragma once

typedef struct ReaderScanColumn {
  const char* name;
  int index;
  unsigned int type;
  signed int type_mode;
  bool remote_only;
} ReaderScanColumn;

typedef struct ReaderScanProjection {
  ReaderScanColumn* columns;
  int ncolumns;
} ReaderScanProjection;
