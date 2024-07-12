GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    doc.go
    mock.go
)

GO_TEST_SRCS(mock_test.go)

END()

RECURSE(
    gotest
)
