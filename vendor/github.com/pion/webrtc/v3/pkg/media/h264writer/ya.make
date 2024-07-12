GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    h264writer.go
)

GO_TEST_SRCS(h264writer_test.go)

END()

RECURSE(
    gotest
)
