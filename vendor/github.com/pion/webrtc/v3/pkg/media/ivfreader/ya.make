GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    ivfreader.go
)

GO_TEST_SRCS(ivfreader_test.go)

END()

RECURSE(
    gotest
)
