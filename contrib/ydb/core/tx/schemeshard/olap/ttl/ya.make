LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
