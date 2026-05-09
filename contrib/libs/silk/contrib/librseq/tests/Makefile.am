# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2022 EfficiOS Inc.

AM_CPPFLAGS += -I$(top_srcdir)/tests/utils

SUBDIRS = utils unit

TEST_EXTENSIONS = .tap
TAP_LOG_DRIVER_FLAGS = --merge --comments
TAP_LOG_DRIVER = env AM_TAP_AWK='$(AWK)' \
	RSEQ_TESTS_SRCDIR='$(abs_top_srcdir)/tests' \
	RSEQ_TESTS_BUILDDIR='$(abs_top_builddir)/tests' \
	$(SHELL) $(srcdir)/utils/tap-driver.sh

noinst_PROGRAMS = \
	basic_percpu_ops_test.tap \
	basic_percpu_ops_test_cxx.tap \
	basic_percpu_ops_mm_cid_test.tap \
	basic_percpu_ops_mm_cid_test_cxx.tap \
	basic_percpu_benchmark.tap \
	basic_percpu_benchmark_cxx.tap \
	basic_percpu_mm_cid_benchmark.tap \
	basic_percpu_mm_cid_benchmark_cxx.tap \
	basic_test.tap \
	basic_test_cxx.tap \
	mempool_test.tap \
	mempool_test_cxx.tap \
	mempool_cow_race_test.tap \
	mempool_cow_race_test_cxx.tap \
	param_test \
	param_test_cxx \
	param_test_mm_cid \
	param_test_mm_cid_cxx \
	param_test_benchmark \
	param_test_benchmark_cxx \
	param_test_mm_cid_benchmark \
	param_test_mm_cid_benchmark_cxx \
	param_test_compare_twice \
	param_test_compare_twice_cxx \
	param_test_mm_cid_compare_twice \
	param_test_mm_cid_compare_twice_cxx \
	no_syscall_test_cxx.tap \
	no_syscall_test.tap \
	syscall_errors_test.tap \
	syscall_errors_test_cxx.tap \
	unregistered_test_cxx.tap \
	unregistered_test.tap

dist_noinst_SCRIPTS = \
	run_no_syscall_test_cxx.tap \
	run_no_syscall_test.tap \
	run_param_test_cxx.tap \
	run_param_test.tap \
	run_syscall_errors_test_cxx.tap \
	run_syscall_errors_test.tap \
	run_unregistered_test_cxx.tap \
	run_unregistered_test.tap

if ENABLE_SHARED
if ENABLE_SECCOMP
noinst_LTLIBRARIES = libdisable-rseq-syscall.la

libdisable_rseq_syscall_la_SOURCES = disable-rseq-syscall.c
libdisable_rseq_syscall_la_LDFLAGS = -module -shared -avoid-version -rpath $(abs_builddir)/.libs/
libdisable_rseq_syscall_la_LIBADD = $(SECCOMP_LIBS)
endif
endif

basic_percpu_ops_test_tap_SOURCES = basic_percpu_ops_test.c
basic_percpu_ops_test_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

basic_percpu_ops_test_cxx_tap_SOURCES = basic_percpu_ops_test_cxx.cpp
basic_percpu_ops_test_cxx_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

basic_percpu_ops_mm_cid_test_tap_SOURCES = basic_percpu_ops_test.c
basic_percpu_ops_mm_cid_test_tap_CPPFLAGS = $(AM_CPPFLAGS) -DBUILDOPT_RSEQ_PERCPU_MM_CID
basic_percpu_ops_mm_cid_test_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

basic_percpu_ops_mm_cid_test_cxx_tap_SOURCES = basic_percpu_ops_test_cxx.cpp
basic_percpu_ops_mm_cid_test_cxx_tap_CPPFLAGS = $(AM_CPPFLAGS) -DBUILDOPT_RSEQ_PERCPU_MM_CID
basic_percpu_ops_mm_cid_test_cxx_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

basic_percpu_benchmark_tap_SOURCES = basic_percpu_benchmark.c
basic_percpu_benchmark_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

basic_percpu_benchmark_cxx_tap_SOURCES = basic_percpu_benchmark_cxx.cpp
basic_percpu_benchmark_cxx_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

basic_percpu_mm_cid_benchmark_tap_SOURCES = basic_percpu_benchmark.c
basic_percpu_mm_cid_benchmark_tap_CPPFLAGS = $(AM_CPPFLAGS) -DBUILDOPT_RSEQ_PERCPU_MM_CID
basic_percpu_mm_cid_benchmark_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

basic_percpu_mm_cid_benchmark_cxx_tap_SOURCES = basic_percpu_benchmark_cxx.cpp
basic_percpu_mm_cid_benchmark_cxx_tap_CPPFLAGS = $(AM_CPPFLAGS) -DBUILDOPT_RSEQ_PERCPU_MM_CID
basic_percpu_mm_cid_benchmark_cxx_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

syscall_errors_test_tap_SOURCES = syscall_errors_test.c
syscall_errors_test_tap_LDADD = $(top_builddir)/tests/utils/libtap.la

syscall_errors_test_cxx_tap_SOURCES = syscall_errors_test_cxx.cpp
syscall_errors_test_cxx_tap_LDADD = $(top_builddir)/tests/utils/libtap.la

unregistered_test_tap_SOURCES = unregistered_test.c
unregistered_test_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

unregistered_test_cxx_tap_SOURCES = unregistered_test_cxx.cpp
unregistered_test_cxx_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

no_syscall_test_tap_SOURCES = no_syscall_test.c
no_syscall_test_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

no_syscall_test_cxx_tap_SOURCES = no_syscall_test_cxx.cpp
no_syscall_test_cxx_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

basic_test_tap_SOURCES = basic_test.c
basic_test_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

basic_test_cxx_tap_SOURCES = basic_test_cxx.cpp
basic_test_cxx_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

mempool_test_tap_SOURCES = mempool_test.c
mempool_test_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

mempool_test_cxx_tap_SOURCES = mempool_test_cxx.cpp
mempool_test_cxx_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

mempool_cow_race_test_tap_SOURCES = mempool_cow_race_test.c
mempool_cow_race_test_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

mempool_cow_race_test_cxx_tap_SOURCES = mempool_cow_race_test_cxx.cpp
mempool_cow_race_test_cxx_tap_LDADD = $(top_builddir)/src/librseq.la $(top_builddir)/tests/utils/libtap.la $(DL_LIBS)

param_test_SOURCES = param_test.c
param_test_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_cxx_SOURCES = param_test_cxx.cpp
param_test_cxx_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_mm_cid_SOURCES = param_test.c
param_test_mm_cid_CPPFLAGS = $(AM_CPPFLAGS) -DBUILDOPT_RSEQ_PERCPU_MM_CID
param_test_mm_cid_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_mm_cid_cxx_SOURCES = param_test_cxx.cpp
param_test_mm_cid_cxx_CPPFLAGS = $(AM_CPPFLAGS) -DBUILDOPT_RSEQ_PERCPU_MM_CID
param_test_mm_cid_cxx_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_benchmark_SOURCES = param_test.c
param_test_benchmark_CPPFLAGS = $(AM_CPPFLAGS) -DBENCHMARK
param_test_benchmark_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_benchmark_cxx_SOURCES = param_test_cxx.cpp
param_test_benchmark_cxx_CPPFLAGS = $(AM_CPPFLAGS) -DBENCHMARK
param_test_benchmark_cxx_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_mm_cid_benchmark_SOURCES = param_test.c
param_test_mm_cid_benchmark_CPPFLAGS = $(AM_CPPFLAGS) -DBENCHMARK -DBUILDOPT_RSEQ_PERCPU_MM_CID
param_test_mm_cid_benchmark_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_mm_cid_benchmark_cxx_SOURCES = param_test_cxx.cpp
param_test_mm_cid_benchmark_cxx_CPPFLAGS = $(AM_CPPFLAGS) -DBENCHMARK -DBUILDOPT_RSEQ_PERCPU_MM_CID
param_test_mm_cid_benchmark_cxx_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_compare_twice_SOURCES = param_test.c
param_test_compare_twice_CPPFLAGS = $(AM_CPPFLAGS) -DRSEQ_COMPARE_TWICE
param_test_compare_twice_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_compare_twice_cxx_SOURCES = param_test_cxx.cpp
param_test_compare_twice_cxx_CPPFLAGS = $(AM_CPPFLAGS) -DRSEQ_COMPARE_TWICE
param_test_compare_twice_cxx_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_mm_cid_compare_twice_SOURCES = param_test.c
param_test_mm_cid_compare_twice_CPPFLAGS = $(AM_CPPFLAGS) -DRSEQ_COMPARE_TWICE -DBUILDOPT_RSEQ_PERCPU_MM_CID
param_test_mm_cid_compare_twice_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

param_test_mm_cid_compare_twice_cxx_SOURCES = param_test_cxx.cpp
param_test_mm_cid_compare_twice_cxx_CPPFLAGS = $(AM_CPPFLAGS) -DRSEQ_COMPARE_TWICE -DBUILDOPT_RSEQ_PERCPU_MM_CID
param_test_mm_cid_compare_twice_cxx_LDADD = $(top_builddir)/src/librseq.la $(DL_LIBS)

# Run shorter tests first
TESTS = \
	unit/arch_mo.tap \
	unit/arch_mo_cxx.tap \
	basic_test.tap \
	basic_test_cxx.tap \
	run_unregistered_test.tap \
	run_unregistered_test_cxx.tap \
	run_syscall_errors_test.tap \
	run_syscall_errors_test_cxx.tap

if ENABLE_SHARED
if ENABLE_SECCOMP
TESTS += \
	run_no_syscall_test.tap \
	run_no_syscall_test_cxx.tap
endif
endif

# Run longer tests last
TESTS += \
	basic_percpu_ops_test.tap \
	basic_percpu_ops_test_cxx.tap \
	basic_percpu_ops_mm_cid_test.tap \
	basic_percpu_ops_mm_cid_test_cxx.tap \
	run_param_test.tap \
	run_param_test_cxx.tap \
	mempool_cow_race_test.tap \
	mempool_cow_race_test_cxx.tap \
	mempool_test.tap \
	mempool_test_cxx.tap
