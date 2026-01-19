OWNER(
    g:yql
    g:yql_ydb_core
)


INCLUDE(header.ya.make.inc)

SET(ORIG_SRC_DIR contrib/ydb/library/yql/minikql/computation)

SET(ORIG_SOURCES
    mkql_computation_node_codegen.cpp
    mkql_simple_codegen.cpp
    mkql_computation_node_graph.cpp
    mkql_computation_node_graph_saveload.cpp
    mkql_computation_node_holders_codegen.cpp
    mkql_llvm_base.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/enumbitset
    library/cpp/packedtypes
    library/cpp/random_provider
    library/cpp/time_provider
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/arrow
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    library/cpp/threading/future
)

YQL_LAST_ABI_VERSION()

ADDINCL(
    ${ORIG_SRC_DIR}
)

COPY(
    WITH_CONTEXT
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
    OUTPUT_INCLUDES 
        ${BINDIR}/contrib/ydb/library/yql/minikql/computation/mkql_llvm_base.h
        ${BINDIR}/contrib/ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h
)

PROVIDES(MINIKQL_COMPUTATION)
