UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

FORK_SUBTESTS()

SRCS(
    utf8_ut.cpp
    yql_issue_ut.cpp
)

PEERDIR(
    library/cpp/unicode/normalization
    contrib/libs/ydb-cpp-sdk/src/library/issue
)

END()
