GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    authority_handler.go
    martianhttp.go
)

GO_TEST_SRCS(
    authority_handler_test.go
    martianhttp_integration_test.go
    martianhttp_test.go
)

END()

RECURSE(
    gotest
)
