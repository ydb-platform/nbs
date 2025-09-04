UNITTEST_FOR(contrib/ydb/core/tx/columnshard/splitter)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/arrow_kernels

    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/columnshard/data_sharing
    contrib/ydb/core/kqp/common
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf
    contrib/ydb/core/persqueue
    contrib/ydb/core/kqp/session_actor
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/tx/columnshard/engines/storage/chunks
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/max
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch
    contrib/ydb/core/tx/columnshard/data_accessor
    contrib/ydb/core/tx
    contrib/ydb/core/mind
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/services/ydb
)

ADDINCL(
    contrib/ydb/library/arrow_clickhouse
)

YQL_LAST_ABI_VERSION()

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    ut_splitter.cpp
    batch_slice.cpp
)

END()
