GO_LIBRARY()

SRCS(
    controller.go
    driver.go
    external_fs.go
    identity.go
    node.go
    helper.go
)

GO_TEST_SRCS(
    controller_test.go
    node_test.go
)

END()

RECURSE_FOR_TESTS(
    mocks
    tests
)
