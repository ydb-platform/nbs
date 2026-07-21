LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/library/aclib
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/services/scheme_secret
)

YQL_LAST_ABI_VERSION()

END()
