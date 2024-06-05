GO_LIBRARY()

SRCS(
    api.go
    fetcher.go
    jwt.go
    token.go
    token_provider.go
)

GO_TEST_SRCS(
    jwt_token_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)