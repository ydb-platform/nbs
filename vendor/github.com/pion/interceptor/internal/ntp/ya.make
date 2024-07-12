GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    ntp.go
)

GO_TEST_SRCS(ntp_test.go)

END()

RECURSE(
    gotest
)
