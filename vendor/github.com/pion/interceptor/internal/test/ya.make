GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    mock_stream.go
    mock_ticker.go
    mock_time.go
)

GO_TEST_SRCS(mock_stream_test.go)

END()

RECURSE(
    gotest
)
