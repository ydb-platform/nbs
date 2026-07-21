INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/minikql/computation/header.ya.make.inc)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/providers/yt/comp_nodes
    contrib/ydb/library/yql/providers/yt/codec
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/utils/failure_injector
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/common
    library/cpp/yson/node
    yt/yt/core
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/public/udf
    contrib/libs/apache/arrow
    contrib/libs/flatbuffers
    contrib/ydb/library/yql/parser/pg_wrapper/interface
)

ADDINCL(
    contrib/libs/flatbuffers/include
)

SET(ORIG_SRC_DIR ${ARCADIA_ROOT}/contrib/ydb/library/yql/providers/yt/comp_nodes/dq)

IF(LINUX)
    PEERDIR(
        yt/yt/client
        contrib/ydb/library/yql/providers/yt/lib/yt_rpc_helpers
    )

    SET(ORIG_SOURCES
        dq_yt_reader.cpp
        dq_yt_factory.cpp
        dq_yt_writer.cpp
        stream_decoder.cpp
        dq_yt_rpc_reader.cpp
        dq_yt_rpc_helpers.cpp
        dq_yt_block_reader.cpp
    )

    CFLAGS(
        -Wno-unused-parameter
    )
ELSE()
    SET(ORIG_SOURCES
        dq_yt_reader.cpp
        dq_yt_factory.cpp
        dq_yt_writer.cpp
    )
ENDIF()

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
)

PROVIDES(YT_COMP_NODES_DQ)
