GO_LIBRARY()

LICENSE(MIT)

SRCS(
    context.go
    filesystem.go
    http.go
    network.go
    utilities.go
)

GO_TEST_SRCS(
    context_test.go
    filesystem_test.go
    http_test.go
    network_test.go
    utilities_test.go
)

END()

RECURSE(
    gotest
)
