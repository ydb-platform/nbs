GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    blob.go
    doc.go
    gc.go
    image.go
    index.go
    layoutpath.go
    options.go
    read.go
    write.go
)

GO_TEST_SRCS(
    gc_test.go
    image_test.go
    index_test.go
    read_test.go
    write_test.go
)

END()

RECURSE(
    gotest
)
