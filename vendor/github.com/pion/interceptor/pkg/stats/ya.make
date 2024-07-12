GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    interceptor.go
    received_stats.go
    sent_stats.go
    stats_recorder.go
)

GO_TEST_SRCS(
    interceptor_test.go
    stats_recorder_test.go
)

END()

RECURSE(
    gotest
)
