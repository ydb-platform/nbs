GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    dpipe.go
)

GO_TEST_SRCS(dpipe_test.go)

END()

RECURSE(
    gotest
)
