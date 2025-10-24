CC		 = gcc
CFLAGS	 = -fPIC -Wall -Wextra -O2 -fno-omit-frame-pointer -DNDEBUG # You can enable latency mesurement mode by adding -DVISTA_MEASURE_LATENCY
CPPFLAGS += -Iinclude
LDLIBS   = -lnuma -luring

export CC CFLAGS CPPFLAGS

LDFLAGS	 = -shared
TARGET   ?= libvista.so

ROOT_SRCS := libvista.c vista_pg_hash.c vista_pg_read_buffer.c vista_pg_buf_ctl.c vista_pg_snapshot.c vista_bitmap.c
ROOT_OBJS := $(ROOT_SRCS:.c=.o)
SUBDIRS := chtable

prefix      ?= /usr
libdir      ?= $(prefix)/lib
includedir  ?= $(prefix)/include

.PHONY: all clean modules

all: $(TARGET)

modules:
	@set -e; \
	for d in $(SUBDIRS); do \
	  $(MAKE) -C $$d CC="$(CC)" CFLAGS="$(CFLAGS)" CPPFLAGS="-I../include -fPIC"; \
	done

$(TARGET): modules $(ROOT_OBJS)
	$(CC) $(LDFLAGS) -o $@ \
	  $(ROOT_OBJS) $(shell find $(SUBDIRS) -name '*.o') $(LDLIBS)


%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

clean:
	$(RM) $(ROOT_OBJS) $(TARGET)
	@for d in $(SUBDIRS); do $(MAKE) -C $$d clean; done

install: $(TARGET)
	@echo "Installing library to $(DESTDIR)$(libdir)"
	@mkdir -p $(DESTDIR)$(libdir)
	@cp -a $(TARGET) $(DESTDIR)$(libdir)/
	@echo "Installing headers to $(DESTDIR)$(includedir)"
	@mkdir -p $(DESTDIR)$(includedir)
	@cp -a include/*.h $(DESTDIR)$(includedir)/

uninstall:
	@echo "Removing installed files"
	@rm -f $(DESTDIR)$(libdir)/$(TARGET)
	@for h in include/*.h; do \
	  rm -f $(DESTDIR)$(includedir)/`basename $$h`; \
	done
