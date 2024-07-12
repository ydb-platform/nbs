GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    catalog.go
    check.go
    delete.go
    descriptor.go
    doc.go
    fetcher.go
    image.go
    index.go
    layer.go
    list.go
    mount.go
    multi_write.go
    options.go
    progress.go
    puller.go
    pusher.go
    referrers.go
    schema1.go
    write.go
)

GO_TEST_SRCS(
    catalog_test.go
    check_test.go
    delete_test.go
    descriptor_test.go
    image_test.go
    index_test.go
    layer_test.go
    list_test.go
    mount_test.go
    multi_write_test.go
    progress_test.go
    schema1_test.go
    write_test.go
)

GO_XTEST_SRCS(
    error_roundtrip_test.go
    referrers_test.go
)

END()

RECURSE(
    gotest
    transport
)
