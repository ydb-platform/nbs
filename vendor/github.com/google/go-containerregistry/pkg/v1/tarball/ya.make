GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

# NB: transitive dependencies
# See https://github.com/golang/go/issues/29258 and st/YMAKE-102 for more details

SRCS(
    doc.go
    image.go
    layer.go
    write.go
)

GO_TEST_SRCS(
    image_test.go
    layer_test.go
)

GO_XTEST_SRCS(
    # progress_test.go
    # write_test.go
)

END()

RECURSE(
    gotest
)
