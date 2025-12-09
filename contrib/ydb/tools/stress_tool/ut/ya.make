IF (SANITIZER_TYPE AND AUTOCHECK)

ELSE()

UNITTEST_FOR(contrib/ydb/tools/stress_tool/lib)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

SRC(
    ../device_test_tool_ut.cpp
)

PEERDIR(
    contrib/ydb/apps/version
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    yql/essentials/minikql/comp_nodes/llvm16
)

END()
ENDIF()
