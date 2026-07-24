# PGXS Makefile used by CI to run Tea extension pg_regress tests from a
# checkout staged under gpdb/gpcontrib/tea/extension.

REGRESS = tea_extension
REGRESS_OPTS = --dbname=$(PG_REGRESS_DB)
DATA = $(wildcard tea.control tea--*.control tea--*.sql)

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
