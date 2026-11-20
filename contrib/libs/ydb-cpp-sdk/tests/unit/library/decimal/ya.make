UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    yql_decimal_ut.cpp
    yql_wide_int_ut.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/library/decimal
)

END()
