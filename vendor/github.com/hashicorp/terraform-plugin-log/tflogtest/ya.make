GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    json_decode.go
    root_logger.go
)

GO_TEST_SRCS(
    json_decode_example_test.go
    root_logger_example_test.go
)

END()

RECURSE(
    gotest
)
