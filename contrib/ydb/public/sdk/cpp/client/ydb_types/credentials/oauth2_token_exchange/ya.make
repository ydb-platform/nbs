LIBRARY()

SRCS(
    credentials.cpp
    from_file.cpp
    jwt_token_source.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    library/cpp/cgiparam
    library/cpp/http/misc
    library/cpp/http/simple
    library/cpp/json
    library/cpp/retry
    library/cpp/uri
    contrib/ydb/public/sdk/cpp/client/ydb_types
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
)

END()

RECURSE_FOR_TESTS(
    ut
)
