INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/minikql/computation/header.ya.make.inc)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/dq/comp_nodes/hash_join_utils
    contrib/ydb/library/yql/dq/runtime/streaming
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/minikql/invoke_builtins
    contrib/ydb/library/yql/utils
)

SET(ORIG_SRC_DIR ${ARCADIA_ROOT}/contrib/ydb/library/yql/dq/comp_nodes)

SET(ORIG_SOURCES
    yql_common_dq_factory.cpp
    dq_hash_combine.cpp
    dq_hash_operator_common.cpp
    dq_hash_operator_serdes.cpp
    dq_program_builder.cpp
    dq_block_hash_join.cpp
    dq_scalar_hash_join.cpp
    dq_hash_join_table.cpp
    dq_join_common.cpp
    dq_watermark_generator.cpp
    type_utils.cpp
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
       ${BINDIR}/contrib/ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h
       ${BINDIR}/contrib/ydb/library/yql/minikql/computation/mkql_llvm_base.h
)

PROVIDES(YDB_DQ_COMP_NODES)
