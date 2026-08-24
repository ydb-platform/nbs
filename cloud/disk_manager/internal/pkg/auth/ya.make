GO_LIBRARY()

SRCS(
    credentials.go
    federated_credentials.go
)

GO_TEST_SRCS(
    credentials_test.go
    federated_credentials_test.go
    rfc8693_credentials_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tests
)
