GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    semaphore.go
)

GO_XTEST_SRCS(
    semaphore_bench_test.go
    semaphore_example_test.go
    semaphore_test.go
)

END()

RECURSE(
    gotest
)
