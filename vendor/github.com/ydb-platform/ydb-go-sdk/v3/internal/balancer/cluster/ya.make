GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    cluster.go
    errors.go
)

GO_TEST_SRCS(cluster_test.go)

END()

RECURSE(
    gotest
)
