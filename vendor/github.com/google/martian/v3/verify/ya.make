GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    verify.go
    verify_handlers.go
)

GO_TEST_SRCS(verify_handlers_test.go)

END()

RECURSE(
    gotest
)
