GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    file.go
    fs.go
)

GO_TEST_SRCS(
    file_test.go
    zipfs_test.go
)

END()

RECURSE(
    gotest
)
