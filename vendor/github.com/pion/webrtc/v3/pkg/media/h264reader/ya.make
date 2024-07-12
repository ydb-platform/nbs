GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    h264reader.go
    nalunittype.go
)

GO_TEST_SRCS(h264reader_test.go)

END()

RECURSE(
    gotest
)
