UNITTEST_FOR(contrib/ydb/core/control/lib)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    util
    contrib/ydb/core/base
)

SRCS(
    immediate_control_board_ut.cpp
)

END()
