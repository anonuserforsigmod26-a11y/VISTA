#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/vista_buffer
#
# IDENTIFICATION
#    src/backend/storage/vista_buffer/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/vista_buffer
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	buf_init.o \
	buf_table.o \
	bufmgr.o \
	freelist.o \
	localbuf.o \
	vista_flushworker.o \
	vista_snapshot.o 

include $(top_srcdir)/src/backend/common.mk
