GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    messages.go
    rawtopicreader.go
    stream_reader_stream_interface.go
)

GO_TEST_SRCS(rawtopicreader_test.go)

END()

RECURSE(
    gotest
)
