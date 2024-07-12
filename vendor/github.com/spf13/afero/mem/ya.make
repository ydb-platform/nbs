GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    dir.go
    dirmap.go
    file.go
)

GO_TEST_SRCS(file_test.go)

END()

RECURSE(
    gotest
)
