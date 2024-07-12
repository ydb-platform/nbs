GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    gunzip.go
    gzip.go
)

GO_TEST_SRCS(
    gunzip_test.go
    gzip_norace_test.go
    gzip_test.go
)

END()

RECURSE(
    gotest
)
