LIBRARY()

SRCS(
    tpc_base.cpp
)

RESOURCE(
    contrib/ydb/library/benchmarks/gen_queries/consts.yql consts.yql
    contrib/ydb/library/benchmarks/gen_queries/consts_decimal.yql consts_decimal.yql
)

PEERDIR(
    library/cpp/resource
    library/cpp/streams/factory/open_by_signature
    contrib/ydb/library/accessor
    contrib/ydb/library/workload/benchmark_base
    contrib/ydb/public/lib/scheme_types
)

GENERATE_ENUM_SERIALIZATION(tpc_base.h)

END()
