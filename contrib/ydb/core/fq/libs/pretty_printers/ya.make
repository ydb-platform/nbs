LIBRARY()

SRCS(
    graph_params_printer.cpp
    minikql_program_printer.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/core/fq/libs/graph_params/proto
    contrib/ydb/library/protobuf_printer
    yql/essentials/minikql
    contrib/ydb/library/yql/providers/dq/api/protos
)

YQL_LAST_ABI_VERSION()

END()
