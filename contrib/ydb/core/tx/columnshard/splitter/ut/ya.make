UNITTEST_FOR(contrib/ydb/core/tx/columnshard/splitter)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/arrow_kernels

    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/core/formats/arrow/compression
    contrib/ydb/core/kqp/common
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf
    contrib/ydb/core/persqueue
    contrib/ydb/core/kqp/session_actor
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/tx
    contrib/ydb/core/mind
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg
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
)

END()
