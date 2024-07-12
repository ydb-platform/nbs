GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    balancerstateaggregator.go
    clustermanager.go
    config.go
    picker.go
)

GO_TEST_SRCS(
    clustermanager_test.go
    config_test.go
)

END()

RECURSE(
    gotest
)
