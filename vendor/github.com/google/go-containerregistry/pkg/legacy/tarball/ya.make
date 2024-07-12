GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    write.go
)

GO_TEST_SRCS(write_test.go)

END()

RECURSE(
    gotest
)
