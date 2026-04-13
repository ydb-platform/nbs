UNITTEST_FOR(contrib/ydb/core/actorlib_impl)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread")
    SPLIT_FACTOR(20)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics/default
    yql/essentials/minikql/comp_nodes/llvm16
)

SRCS(
    actor_activity_ut.cpp
    actor_bootstrapped_ut.cpp
    actor_tracker_ut.cpp
    test_interconnect_ut.cpp
    test_protocols_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
