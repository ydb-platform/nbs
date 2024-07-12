GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    diffcopy.go
    filesync.go
    filesync.pb.go
    generate.go
)

GO_TEST_SRCS(filesync_test.go)

END()

RECURSE(
    gotest
)
