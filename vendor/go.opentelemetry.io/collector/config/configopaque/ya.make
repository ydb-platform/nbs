GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    opaque.go
)

GO_TEST_SRCS(opaque_test.go)

GO_XTEST_SRCS(doc_test.go)

END()

RECURSE(
    gotest
)
