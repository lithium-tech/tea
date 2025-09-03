#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"

#include "tea/gpext/tea_reader.h"

static bool reader_initialized = false;
static List *readers = NULL;

PG_MODULE_MAGIC;

static void DestroyAllReaders() {
  ListCell *lc = NULL;
  foreach (lc, readers) {
    TeaContextPtr ctx = lfirst(lc);
    TeaContextLogStats(ctx, "CANCELLED_QUERY");
    TeaContextDestroyUntracked(ctx);
  }
  list_free(readers);
  readers = NULL;
}

static void XactCallbackTea(XactEvent event, void *arg) {
  switch (event) {
    case XACT_EVENT_ABORT:
      DestroyAllReaders();
      break;
    case XACT_EVENT_COMMIT: {
      int readers_count = list_length(readers);
      if (readers_count > 0) {
        elog(LOG, "There are %d open readers", readers_count);
      }
      break;
    }
    default:
      break;
  }
}

static void OnExitCallback(int code, Datum arg) {
  UnregisterXactCallback(XactCallbackTea, NULL);
  DestroyAllReaders();
  TeaContextFinalize();
}

TeaContextPtr TeaContextCreate(const char *url) {
  if (!reader_initialized) {
    TeaContextInitialize(GetDatabaseEncoding());
    RegisterXactCallback(XactCallbackTea, NULL);
    on_shmem_exit(OnExitCallback, 0);
    reader_initialized = true;
  }
  TeaContextPtr tea_ctx = TeaContextCreateUntracked(url);
  if (tea_ctx) {
    MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    readers = lappend(readers, tea_ctx);
    MemoryContextSwitchTo(oldcontext);
  }
  return tea_ctx;
}

void TeaContextDestroy(TeaContextPtr tea_ctx) {
  TeaContextDestroyUntracked(tea_ctx);
  readers = list_delete_ptr(readers, tea_ctx);
}
