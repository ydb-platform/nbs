GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    ivfwriter.go
)

GO_TEST_SRCS(ivfwriter_test.go)

END()

RECURSE(
    gotest
)
