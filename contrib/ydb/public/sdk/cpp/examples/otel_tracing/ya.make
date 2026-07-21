PROGRAM(otel_tracing_example)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/impl/observability
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/params
    contrib/ydb/public/sdk/cpp/plugins/trace/otel
    contrib/ydb/public/sdk/cpp/plugins/metrics/otel
    contrib/libs/opentelemetry-cpp
    contrib/libs/opentelemetry-cpp/api
)

END()
