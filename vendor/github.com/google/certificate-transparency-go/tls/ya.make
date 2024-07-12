GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    signature.go
    tls.go
    types.go
)

GO_TEST_SRCS(
    # hash_test.go
    # tls_test.go
    # types_test.go
)

GO_XTEST_SRCS(
    # signature_test.go
)

END()

RECURSE(
    gotest
)
