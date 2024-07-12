GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    oggwriter.go
)

GO_TEST_SRCS(oggwriter_test.go)

END()

RECURSE(
    gotest
)
