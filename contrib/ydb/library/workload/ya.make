LIBRARY()

PEERDIR(
    contrib/ydb/library/workload/abstract
    contrib/ydb/library/workload/clickbench
    contrib/ydb/library/workload/query
    contrib/ydb/library/workload/kv
    contrib/ydb/library/workload/log
    contrib/ydb/library/workload/mixed
    contrib/ydb/library/workload/stock
    contrib/ydb/library/workload/tpcc
    contrib/ydb/library/workload/tpcds
    contrib/ydb/library/workload/tpch
    contrib/ydb/library/workload/vector
)

END()

RECURSE(
    abstract
    benchmark_base
    clickbench
    query
    kv
    log
    mixed
    stock
    tpcc
    tpc_base
    tpcds
    tpch
)
