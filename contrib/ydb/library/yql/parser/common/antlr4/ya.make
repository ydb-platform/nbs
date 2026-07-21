LIBRARY()

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
    contrib/ydb/library/yql/parser/common
)

SRCS(
    error_listener.cpp
)

END()
