GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    access_token.go
    anonymous.go
    credentials.go
    errors.go
    oauth2.go
    source_info.go
    static.go
)

GO_TEST_SRCS(
    credentials_test.go
    errors_test.go
    oauth2_test.go
    static_test.go
)

END()

RECURSE(
    gotest
)
