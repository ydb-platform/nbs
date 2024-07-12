GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    append.go
    catalog.go
    config.go
    copy.go
    delete.go
    digest.go
    doc.go
    export.go
    filemap.go
    get.go
    list.go
    manifest.go
    options.go
    pull.go
    push.go
    tag.go
)

GO_TEST_SRCS(
    digest_test.go
    export_test.go
    options_test.go
)

GO_XTEST_SRCS(
    append_test.go
    crane_test.go
    example_test.go
    filemap_test.go
)

END()

RECURSE(
    gotest
)
