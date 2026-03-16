EXTENSION = mini_audit_pro
MODULE_big = mini_audit_pro
OBJS = src/mini_audit_pro.o

DATA = sql/mini_audit_pro--1.0.sql
PGFILEDESC = "mini_audit_pro - async audit logger"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/mini_audit_pro
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
ifeq ($(top_srcdir),)
include $(top_builddir)/contrib/contrib-global.mk
else
include $(top_srcdir)/contrib/contrib-global.mk
endif
endif
