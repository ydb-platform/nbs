GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    worker.go
)

GO_TEST_SRCS(worker_test.go)

END()

RECURSE(
    gotest
)
