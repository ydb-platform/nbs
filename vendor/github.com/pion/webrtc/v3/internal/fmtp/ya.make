GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    fmtp.go
    h264.go
)

GO_TEST_SRCS(
    fmtp_test.go
    h264_test.go
)

END()

RECURSE(
    gotest
)
