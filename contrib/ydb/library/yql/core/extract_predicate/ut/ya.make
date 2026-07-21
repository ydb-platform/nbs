UNITTEST_FOR(contrib/ydb/library/yql/core/extract_predicate)

SRCS(
    extract_predicate_ut.cpp
)

PEERDIR(
    library/cpp/yson
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/providers/config
    contrib/ydb/library/yql/providers/pure
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/result/provider
    contrib/ydb/library/yql/core/cbo/simple
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/lexer/antlr4
    contrib/ydb/library/yql/sql/v1/lexer/antlr4_ansi
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4
    contrib/ydb/library/yql/sql/v1/proto_parser/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
