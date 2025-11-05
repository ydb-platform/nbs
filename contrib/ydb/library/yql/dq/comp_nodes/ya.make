LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/utils
)

SRCS(
    yql_common_dq_factory.cpp
)

YQL_LAST_ABI_VERSION()


END()

