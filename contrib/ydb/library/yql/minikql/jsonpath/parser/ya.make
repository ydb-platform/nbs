LIBRARY()

IF (ARCH_X86_64)
    CFLAGS(
        -DYDB_REWRAPPER_LIB_ID=kHyperscan
    )
ELSE()
    CFLAGS(
        -DYDB_REWRAPPER_LIB_ID=kRe2
    )

ENDIF()

PEERDIR(
    contrib/libs/double-conversion
    library/cpp/json
    contrib/ydb/library/yql/minikql/jsonpath/rewrapper
    contrib/ydb/library/yql/minikql/jsonpath/rewrapper/re2
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/parser/proto_ast/antlr3
    contrib/ydb/library/yql/parser/proto_ast/gen/jsonpath
)

SRCS(
    ast_builder.cpp
    ast_nodes.cpp
    binary.cpp
    parser.cpp
    parse_double.cpp
    type_check.cpp
)

GENERATE_ENUM_SERIALIZATION(ast_nodes.h)

END()

