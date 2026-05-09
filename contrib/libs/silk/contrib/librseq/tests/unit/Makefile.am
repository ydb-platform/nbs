# SPDX-FileCopyrightText: 2023 EfficiOS Inc.
#
# SPDX-License-Identifier: MIT

AM_CPPFLAGS += -I$(top_srcdir)/tests/utils

noinst_PROGRAMS = \
	arch_mo.tap \
	arch_mo_cxx.tap

arch_mo_tap_SOURCES = arch-mo.c
arch_mo_tap_LDADD = $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

arch_mo_cxx_tap_SOURCES = arch-mo-cxx.cpp
arch_mo_cxx_tap_LDADD = $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)
