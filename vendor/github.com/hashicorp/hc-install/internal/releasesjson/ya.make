GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    checksum_downloader.go
    downloader.go
    product_version.go
    releases.go
)

GO_TEST_SRCS(releases_test.go)

END()

RECURSE(
    gotest
)
