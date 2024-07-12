GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    mock.go
)

GO_TEST_SRCS(mock_test.go)

END()

RECURSE(
    gotest
)
