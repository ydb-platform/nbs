GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auxiliary.go
    doc.go
    from_conn.go
    info.go
    operations_client.go
)

GO_XTEST_SRCS(operations_client_example_test.go)

END()

RECURSE(
    gotest
    longrunningpb
)
