PROGRAM(metric_buffer_benchmark)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/public/sdk/cpp/src/client/impl/observability
    contrib/ydb/public/sdk/cpp/src/client/metrics
)

END()
