# Generated by devtools/yamaker.

INCLUDE(${ARCADIA_ROOT}/build/platform/clang/arch.cmake)

LIBRARY(clang_rt.lsan${CLANG_RT_SUFFIX})

LICENSE(
    Apache-2.0 AND
    Apache-2.0 WITH LLVM-exception AND
    MIT AND
    NCSA
)

LICENSE_TEXTS(../../LICENSE.TXT)

OWNER(g:cpp-contrib)

ADDINCL(
    contrib/libs/clang16-rt/lib
)

NO_COMPILER_WARNINGS()

NO_UTIL()

NO_SANITIZE()

CFLAGS(
    -DHAVE_RPC_XDR_H=0
    -fno-builtin
    -fno-exceptions
    -fno-lto
    -fno-rtti
    -fno-stack-protector
    -fomit-frame-pointer
    -funwind-tables
    -fvisibility=hidden
)

SRCDIR(contrib/libs/clang16-rt/lib)

SRCS(
    interception/interception_linux.cpp
    interception/interception_mac.cpp
    interception/interception_type_test.cpp
    interception/interception_win.cpp
    lsan/lsan.cpp
    lsan/lsan_allocator.cpp
    lsan/lsan_common.cpp
    lsan/lsan_common_fuchsia.cpp
    lsan/lsan_common_linux.cpp
    lsan/lsan_common_mac.cpp
    lsan/lsan_fuchsia.cpp
    lsan/lsan_interceptors.cpp
    lsan/lsan_linux.cpp
    lsan/lsan_mac.cpp
    lsan/lsan_malloc_mac.cpp
    lsan/lsan_posix.cpp
    lsan/lsan_preinit.cpp
    lsan/lsan_thread.cpp
    sanitizer_common/sancov_flags.cpp
    sanitizer_common/sanitizer_allocator.cpp
    sanitizer_common/sanitizer_allocator_checks.cpp
    sanitizer_common/sanitizer_allocator_report.cpp
    sanitizer_common/sanitizer_chained_origin_depot.cpp
    sanitizer_common/sanitizer_common.cpp
    sanitizer_common/sanitizer_common_libcdep.cpp
    sanitizer_common/sanitizer_coverage_fuchsia.cpp
    sanitizer_common/sanitizer_coverage_libcdep_new.cpp
    sanitizer_common/sanitizer_coverage_win_sections.cpp
    sanitizer_common/sanitizer_deadlock_detector1.cpp
    sanitizer_common/sanitizer_deadlock_detector2.cpp
    sanitizer_common/sanitizer_errno.cpp
    sanitizer_common/sanitizer_file.cpp
    sanitizer_common/sanitizer_flag_parser.cpp
    sanitizer_common/sanitizer_flags.cpp
    sanitizer_common/sanitizer_fuchsia.cpp
    sanitizer_common/sanitizer_libc.cpp
    sanitizer_common/sanitizer_libignore.cpp
    sanitizer_common/sanitizer_linux.cpp
    sanitizer_common/sanitizer_linux_libcdep.cpp
    sanitizer_common/sanitizer_linux_s390.cpp
    sanitizer_common/sanitizer_mac.cpp
    sanitizer_common/sanitizer_mac_libcdep.cpp
    sanitizer_common/sanitizer_mutex.cpp
    sanitizer_common/sanitizer_netbsd.cpp
    sanitizer_common/sanitizer_platform_limits_freebsd.cpp
    sanitizer_common/sanitizer_platform_limits_linux.cpp
    sanitizer_common/sanitizer_platform_limits_netbsd.cpp
    sanitizer_common/sanitizer_platform_limits_posix.cpp
    sanitizer_common/sanitizer_platform_limits_solaris.cpp
    sanitizer_common/sanitizer_posix.cpp
    sanitizer_common/sanitizer_posix_libcdep.cpp
    sanitizer_common/sanitizer_printf.cpp
    sanitizer_common/sanitizer_procmaps_bsd.cpp
    sanitizer_common/sanitizer_procmaps_common.cpp
    sanitizer_common/sanitizer_procmaps_fuchsia.cpp
    sanitizer_common/sanitizer_procmaps_linux.cpp
    sanitizer_common/sanitizer_procmaps_mac.cpp
    sanitizer_common/sanitizer_procmaps_solaris.cpp
    sanitizer_common/sanitizer_solaris.cpp
    sanitizer_common/sanitizer_stack_store.cpp
    sanitizer_common/sanitizer_stackdepot.cpp
    sanitizer_common/sanitizer_stacktrace.cpp
    sanitizer_common/sanitizer_stacktrace_libcdep.cpp
    sanitizer_common/sanitizer_stacktrace_printer.cpp
    sanitizer_common/sanitizer_stacktrace_sparc.cpp
    sanitizer_common/sanitizer_stoptheworld_fuchsia.cpp
    sanitizer_common/sanitizer_stoptheworld_linux_libcdep.cpp
    sanitizer_common/sanitizer_stoptheworld_mac.cpp
    sanitizer_common/sanitizer_stoptheworld_netbsd_libcdep.cpp
    sanitizer_common/sanitizer_stoptheworld_win.cpp
    sanitizer_common/sanitizer_suppressions.cpp
    sanitizer_common/sanitizer_symbolizer.cpp
    sanitizer_common/sanitizer_symbolizer_libbacktrace.cpp
    sanitizer_common/sanitizer_symbolizer_libcdep.cpp
    sanitizer_common/sanitizer_symbolizer_mac.cpp
    sanitizer_common/sanitizer_symbolizer_markup.cpp
    sanitizer_common/sanitizer_symbolizer_posix_libcdep.cpp
    sanitizer_common/sanitizer_symbolizer_report.cpp
    sanitizer_common/sanitizer_symbolizer_win.cpp
    sanitizer_common/sanitizer_termination.cpp
    sanitizer_common/sanitizer_thread_registry.cpp
    sanitizer_common/sanitizer_tls_get_addr.cpp
    sanitizer_common/sanitizer_type_traits.cpp
    sanitizer_common/sanitizer_unwind_linux_libcdep.cpp
    sanitizer_common/sanitizer_unwind_win.cpp
    sanitizer_common/sanitizer_win.cpp
)

END()
