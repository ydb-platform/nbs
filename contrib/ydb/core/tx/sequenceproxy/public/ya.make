LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/library/aclib
    contrib/ydb/public/api/protos
    contrib/ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
