LIBRARY()

SRCS(
    registry.cpp
    program.cpp
    builder.cpp
    resolver.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/formats/arrow/filter
    contrib/ydb/core/formats/arrow/printer
    contrib/ydb/core/formats/arrow/program
    contrib/ydb/core/protos
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/core/arrow_kernels/registry
)

YQL_LAST_ABI_VERSION()

END()
