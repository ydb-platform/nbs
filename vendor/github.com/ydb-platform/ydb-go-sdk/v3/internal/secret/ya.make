GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    password.go
    token.go
)

GO_TEST_SRCS(
    password_test.go
    token_test.go
)

END()

RECURSE(
    gotest
)
