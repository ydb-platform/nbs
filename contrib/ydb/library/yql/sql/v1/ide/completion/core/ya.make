LIBRARY()

SRCS(
    environment.cpp
    input.cpp
    name.cpp
    position.cpp
    statement.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/pure_ast
    contrib/ydb/library/yql/core/sql_types
    library/cpp/yson/node
)

END()
