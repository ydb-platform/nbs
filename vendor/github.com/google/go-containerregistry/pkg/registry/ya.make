GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    blobs.go
    blobs_disk.go
    error.go
    manifest.go
    registry.go
    tls.go
)

GO_TEST_SRCS(depcheck_test.go)

GO_XTEST_SRCS(
    blobs_disk_test.go
    compatibility_test.go
    registry_test.go
    tls_test.go
)

END()

RECURSE(
    gotest
)
