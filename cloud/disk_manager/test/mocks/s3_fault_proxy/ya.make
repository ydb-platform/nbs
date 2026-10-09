GO_LIBRARY()

SRCS(
    controller.go
    proxy.go
)

GO_TEST_SRCS(
    controller_test.go
    proxy_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)

RECURSE(
    cmd/s3_fault_proxy
)
