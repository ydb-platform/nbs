PROGRAM(cbo_latency_dataset)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/testing/common
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/opt/cbo/bench
    contrib/ydb/core/kqp/opt/cbo/solver
    contrib/ydb/core/testlib
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/udfs/common/digest
)

YQL_LAST_ABI_VERSION()

END()
