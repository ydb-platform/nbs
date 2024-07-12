GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    file.go
    sftp.go
)

GO_TEST_SRCS(sftp_test.go)

END()

RECURSE(
    gotest
)
