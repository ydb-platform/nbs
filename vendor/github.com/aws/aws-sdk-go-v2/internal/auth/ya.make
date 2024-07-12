GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auth.go
    scheme.go
)

GO_TEST_SRCS(scheme_test.go)

END()

RECURSE(
    gotest
    smithy
)
