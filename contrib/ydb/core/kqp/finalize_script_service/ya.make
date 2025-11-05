LIBRARY()

SRCS(
    kqp_finalize_script_actor.cpp
    kqp_finalize_script_service.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/proxy_service
    contrib/ydb/library/yql/providers/s3/actors_factory
)

YQL_LAST_ABI_VERSION()

END()
