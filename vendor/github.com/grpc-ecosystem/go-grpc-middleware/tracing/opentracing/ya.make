GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    client_interceptors.go
    doc.go
    id_extract.go
    metadata.go
    options.go
    server_interceptors.go
)

GO_TEST_SRCS(id_extract_test.go)

GO_XTEST_SRCS(interceptors_test.go)

END()

RECURSE(
    # gotest
)
