GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    buffer.go
    copy.go
    fetcher.go
    multiprovider.go
    pusher.go
    refs.go
    source.go
    types.go
)

GO_TEST_SRCS(
    buffer_test.go
    copy_test.go
    fetcher_test.go
    multiprovider_test.go
    source_test.go
)

END()

RECURSE(
    gotest
)
