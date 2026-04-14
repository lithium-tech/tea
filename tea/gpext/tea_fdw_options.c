#include "tea/gpext/tea_fdw_options.h"

#include "commands/defrem.h"
#include "foreign/foreign.h"

char* TeaGetLocation(Oid foreigntableid) {
  ForeignTable* table;
  ListCell* lc;

  table = GetForeignTable(foreigntableid);

  foreach (lc, table->options) {
    DefElem* def = (DefElem*)lfirst(lc);

    if (strcmp(def->defname, "location") == 0) {
      return defGetString(def);
    }
  }
  return NULL;
}
