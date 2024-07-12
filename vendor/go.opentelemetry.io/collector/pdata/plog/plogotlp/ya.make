GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    generated_exportpartialsuccess.go
    grpc.go
    request.go
    response.go
)

GO_TEST_SRCS(
    generated_exportpartialsuccess_test.go
    grpc_test.go
    request_test.go
    response_test.go
)

END()

RECURSE(
    gotest
)
