GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    doc.go
    hash.go
    image.go
    index.go
    layer.go
    manifest.go
    platform.go
    progress.go
    zz_deepcopy_generated.go
)

GO_TEST_SRCS(
    config_test.go
    hash_test.go
    manifest_test.go
)

GO_XTEST_SRCS(platform_test.go)

END()

RECURSE(
    cache
    daemon
    empty
    fake
    google
    gotest
    layout
    match
    mutate
    partial
    random
    remote
    static
    stream
    tarball
    types
    validate
)
