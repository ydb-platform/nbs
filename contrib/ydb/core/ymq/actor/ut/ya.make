UNITTEST()

PEERDIR(
    contrib/libs/yaml-cpp
    contrib/ydb/core/mind/address_classification
    contrib/ydb/core/testlib/default
    contrib/ydb/core/ymq/actor
    contrib/ydb/core/ymq/base
    contrib/ydb/core/ymq/http
)

SRCS(
    infly_ut.cpp
    message_delay_stats_ut.cpp
    sha256_ut.cpp
    metering_ut.cpp
    source_address_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
