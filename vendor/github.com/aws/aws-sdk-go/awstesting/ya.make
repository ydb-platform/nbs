GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    assert.go
    certificate_utils.go
    client.go
    discard.go
    endless_reader.go
    util.go
)

GO_XTEST_SRCS(
    assert_test.go
    util_test.go
)

END()

RECURSE(
    gotest
    mock
    unit
)
