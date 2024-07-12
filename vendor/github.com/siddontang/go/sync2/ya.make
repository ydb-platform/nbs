GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    atomic.go
    semaphore.go
)

GO_TEST_SRCS(
    atomic_test.go
    semaphore_test.go
)

END()

RECURSE(
    gotest
)
