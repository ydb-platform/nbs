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
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
)

END()
ENDIF()
