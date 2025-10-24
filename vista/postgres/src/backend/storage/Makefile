#
# Makefile for the storage manager subsystem
#
# src/backend/storage/Makefile
#

subdir = src/backend/storage
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(findstring -DVISTA, $(CFLAGS)),-DVISTA)
SUBDIRS     = vista_buffer file freespace ipc large_object lmgr page smgr sync
else
SUBDIRS     = buffer file freespace ipc large_object lmgr page smgr sync
endif

include $(top_srcdir)/src/backend/common.mk
