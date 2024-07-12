GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    interfaces.go
    mock.go
    overlap.go
)

GO_TEST_SRCS(overlap_test.go)

END()

RECURSE(
    gotest
)
