GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    dictionary.go
    read.go
    types.go
    write.go
)

GO_TEST_SRCS(spdy_test.go)

END()

RECURSE(
    gotest
)
