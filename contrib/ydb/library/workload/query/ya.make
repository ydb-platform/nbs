LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    data_generator.cpp
    query.cpp
)

PEERDIR(
    contrib/ydb/library/workload/benchmark_base
    library/cpp/json
)

END()
