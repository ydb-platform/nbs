UNITTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    yql_decimal_ut.cpp
    yql_wide_int_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/decimal
)

END()
