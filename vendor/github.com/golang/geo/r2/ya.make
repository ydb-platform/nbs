GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    rect.go
)

GO_TEST_SRCS(rect_test.go)

END()

RECURSE(
    gotest
)
