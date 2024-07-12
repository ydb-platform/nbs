GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    doc.go
    extractor.go
    oauth2.go
    request.go
)

GO_TEST_SRCS(
    extractor_example_test.go
    extractor_test.go
    # request_test.go
)

END()

RECURSE(
    gotest
)
