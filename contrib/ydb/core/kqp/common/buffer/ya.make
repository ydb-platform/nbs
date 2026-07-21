LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
