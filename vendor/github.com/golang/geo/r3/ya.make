GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    precisevector.go
    vector.go
)

GO_TEST_SRCS(
    precisevector_test.go
    vector_test.go
)

END()

RECURSE(
    gotest
)
