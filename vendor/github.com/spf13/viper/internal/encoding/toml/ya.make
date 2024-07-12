GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    codec.go
)

GO_TEST_SRCS(codec_test.go)

END()

RECURSE(
    gotest
)
