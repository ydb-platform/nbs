GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    discovery.go
)

GO_TEST_SRCS(
    discovery_test.go
    grpc_client_mock_test.go
)

END()

RECURSE(
    config
    gotest
)
