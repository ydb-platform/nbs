GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    access_error.go
    access_token.go
    anonymous.go
    credentials.go
    oauth2.go
    source_info.go
    static.go
)

GO_TEST_SRCS(
    access_error_test.go
    credentials_test.go
    static_test.go
)

END()

RECURSE(
    gotest
)
