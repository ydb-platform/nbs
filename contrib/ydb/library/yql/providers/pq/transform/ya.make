LIBRARY()

SRCS(
    yql_pq_dq_transform.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/pq/common
    contrib/ydb/library/yql/providers/pq/proto

    contrib/ydb/library/yql/minikql
)

YQL_LAST_ABI_VERSION()

END()
