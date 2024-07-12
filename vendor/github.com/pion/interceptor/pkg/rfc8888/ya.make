GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    interceptor.go
    option.go
    recorder.go
    stream_log.go
    ticker.go
    unwrapper.go
)

GO_TEST_SRCS(
    interceptor_test.go
    recorder_test.go
    stream_log_test.go
    unwrapper_test.go
)

END()

RECURSE(
    gotest
)
