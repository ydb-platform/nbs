LIBRARY()

SRCS(
    ast.cpp
    check_format.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/format
)

END()

RECURSE_FOR_TESTS(ut)
