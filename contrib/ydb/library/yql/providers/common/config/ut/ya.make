UNITTEST_FOR(contrib/ydb/library/yql/providers/common/config)

SRCS(
    yql_config_ut.cpp
    yql_config_qplayer_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/qplayer/storage/memory
)

YQL_LAST_ABI_VERSION()

END()
