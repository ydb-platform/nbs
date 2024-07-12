GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    reader.go
    rtpdump.go
    writer.go
)

GO_TEST_SRCS(
    reader_test.go
    rtpdump_test.go
    writer_test.go
)

END()

RECURSE(
    gotest
)
