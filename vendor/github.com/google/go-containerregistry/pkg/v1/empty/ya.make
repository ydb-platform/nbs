GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    image.go
    index.go
)

GO_TEST_SRCS(
    image_test.go
    index_test.go
)

END()

RECURSE(
    gotest
)
