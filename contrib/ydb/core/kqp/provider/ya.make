LIBRARY()

SRCS(
    read_attributes_utils.cpp
    rewrite_io_utils.cpp
    yql_kikimr_constraints.cpp
    yql_kikimr_datasink.cpp
    yql_kikimr_datasource.cpp
    yql_kikimr_exec.cpp
    yql_kikimr_expr_nodes.h
    yql_kikimr_expr_nodes.cpp
    yql_kikimr_gateway.h
    yql_kikimr_gateway.cpp
    yql_kikimr_opt_build.cpp
    yql_kikimr_opt.cpp
    yql_kikimr_provider.h
    yql_kikimr_provider.cpp
    yql_kikimr_provider_impl.h
    yql_kikimr_results.cpp
    yql_kikimr_results.h
    yql_kikimr_settings.cpp
    yql_kikimr_settings.h
    yql_kikimr_type_ann.cpp
    yql_kikimr_type_ann_pg.h
    yql_kikimr_type_ann_pg.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/docapi
    contrib/ydb/core/kqp/expr_nodes
    contrib/ydb/core/kqp/opt/cbo
    contrib/ydb/core/local_indexes/bloom
    contrib/ydb/core/kqp/query_data
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/min_max/misc
    contrib/ydb/library/aclib
    contrib/ydb/library/aclib/protos
    contrib/ydb/library/ydb_issue/proto
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/constraints
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/public/lib/scheme_types
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/services/metadata/optimization
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/core/peephole_opt
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/config
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/common/transform
    contrib/ydb/library/yql/providers/pg/expr_nodes
    contrib/ydb/library/yql/providers/result/expr_nodes
    contrib/ydb/library/yql/providers/result/provider
    contrib/ydb/library/yql/public/decimal
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/types/binary_json
    contrib/ydb/library/yql/types/dynumber
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

SRCDIR(contrib/ydb/library/yql/core/expr_nodes_gen)

IF(EXPORT_CMAKE)
    RUN_PYTHON3(
        ${ARCADIA_ROOT}/contrib/ydb/library/yql/core/expr_nodes_gen/gen/__main__.py
            yql_expr_nodes_gen.jnj
            yql_kikimr_expr_nodes.json
            yql_kikimr_expr_nodes.gen.h
            yql_kikimr_expr_nodes.decl.inl.h
            yql_kikimr_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_kikimr_expr_nodes.json
        OUT yql_kikimr_expr_nodes.gen.h
        OUT yql_kikimr_expr_nodes.decl.inl.h
        OUT yql_kikimr_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/contrib/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ELSE()
    RUN_PROGRAM(
        contrib/ydb/library/yql/core/expr_nodes_gen/gen
            yql_expr_nodes_gen.jnj
            yql_kikimr_expr_nodes.json
            yql_kikimr_expr_nodes.gen.h
            yql_kikimr_expr_nodes.decl.inl.h
            yql_kikimr_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_kikimr_expr_nodes.json
        OUT yql_kikimr_expr_nodes.gen.h
        OUT yql_kikimr_expr_nodes.decl.inl.h
        OUT yql_kikimr_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/contrib/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ENDIF()

GENERATE_ENUM_SERIALIZATION(yql_kikimr_provider.h)
GENERATE_ENUM_SERIALIZATION(yql_kikimr_gateway.h)

END()

RECURSE_FOR_TESTS(
    ut
)
