UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

FORK_SUBTESTS()

SRCS(
    utf8_ut.cpp
    yql_issue_ut.cpp
)

PEERDIR(
    library/cpp/unicode/normalization
    contrib/ydb/public/sdk/cpp/src/library/issue
)

END()
