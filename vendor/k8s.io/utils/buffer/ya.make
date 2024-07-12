GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    ring_growing.go
)

GO_TEST_SRCS(ring_growing_test.go)

END()

RECURSE(
    gotest
)
