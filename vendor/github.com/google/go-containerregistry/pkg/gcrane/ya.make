GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    copy.go
    doc.go
    options.go
)

GO_TEST_SRCS(
    copy_test.go
    options_test.go
)

END()

RECURSE(
    gotest
)
