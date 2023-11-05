LIBRARY()

SRCS(
    log.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/protos
    contrib/ydb/library/yql/utils/actor_log
)

YQL_LAST_ABI_VERSION()

END()
