LIBRARY()

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    dq_pq_meta_extractor.cpp
    dq_pq_read_actor.cpp
    dq_pq_write_actor.cpp
    probes.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/public/types
    contrib/ydb/library/yql/utils/log
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/library/yql/providers/pq/common
)

YQL_LAST_ABI_VERSION()

END()
