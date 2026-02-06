LIBRARY()

PEERDIR(
    contrib/ydb/library/workload/abstract
    contrib/ydb/library/workload/clickbench
    contrib/ydb/library/workload/kv
    contrib/ydb/library/workload/log
    contrib/ydb/library/workload/stock
    contrib/ydb/library/workload/tpcds
    contrib/ydb/library/workload/tpch
)

END()

RECURSE(
    abstract
    benchmark_base
    clickbench
    kv
    log
    stock
    tpc_base
    tpcds
    tpch
)