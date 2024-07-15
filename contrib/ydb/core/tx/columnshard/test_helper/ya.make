LIBRARY()

PEERDIR(
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/blobs_action/bs
    contrib/ydb/core/tx/columnshard/blobs_action/tier
    contrib/ydb/core/wrappers
)

SRCS(
    helper.cpp
    controllers.cpp
)

YQL_LAST_ABI_VERSION()

END()

