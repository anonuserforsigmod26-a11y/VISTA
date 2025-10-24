
PG_INSTALL_DIR = $(CURDIR)/../../pgsql

PG_CONFIG = $(PG_INSTALL_DIR)/bin/pg_config 

PG_CPPFLAGS += -I$(CURDIR)/../../libvista/include
PG_LIBS = $(BE_DLLLIBS)

MODULES = vista_udf
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
