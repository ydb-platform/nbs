GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    elliptic.go
)

GO_TEST_SRCS(elliptic_test.go)

END()

RECURSE(
    gotest
)
