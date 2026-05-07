UNITTEST_FOR(contrib/ydb/core/client)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    REQUIREMENTS(
        ram:32
    )
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/client/scheme_cache_lib
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tablet_flat/test/libs/rows
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/supp/ubsan_supp.inc)

SRCS(
    cancel_tx_ut.cpp
    flat_ut.cpp
    locks_ut.cpp
    object_storage_listing_ut.cpp
)

END()
