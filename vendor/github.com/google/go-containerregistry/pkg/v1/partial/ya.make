GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

# NB: transitive dependencies
# See https://github.com/golang/go/issues/29258 and st/YMAKE-102 for more details

SRCS(
    compressed.go
    doc.go
    image.go
    index.go
    uncompressed.go
    with.go
)

GO_TEST_SRCS(configlayer_test.go)

GO_XTEST_SRCS(
    # compressed_test.go
    # index_test.go
    # uncompressed_test.go
    # with_test.go
)

END()

RECURSE(
    gotest
)
