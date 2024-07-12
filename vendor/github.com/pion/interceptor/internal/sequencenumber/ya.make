GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    unwrapper.go
)

GO_TEST_SRCS(unwrapper_test.go)

END()

RECURSE(
    gotest
)
