GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    prf.go
)

GO_TEST_SRCS(prf_test.go)

END()

RECURSE(
    gotest
)
