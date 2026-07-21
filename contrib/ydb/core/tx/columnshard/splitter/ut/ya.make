UNITTEST_FOR(contrib/ydb/core/tx/columnshard/splitter)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/accessor/dictionary
    contrib/ydb/core/formats/arrow/accessor/plain
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/session_actor
    contrib/ydb/core/mind
    contrib/ydb/core/tx
    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/columnshard/data_sharing
    contrib/ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    contrib/ydb/core/tx/columnshard/engines/storage/chunks
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/max
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/min_max
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    contrib/ydb/core/tx/columnshard/data_accessor
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/library/arrow_kernels
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/services/ydb
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
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
