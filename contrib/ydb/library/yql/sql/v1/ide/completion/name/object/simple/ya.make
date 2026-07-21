LIBRARY()

SRCS(
    schema.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object
    library/cpp/case_insensitive_string
)

END()

RECURSE(
    static
)

RECURSE_FOR_TESTS(
    ut
)
