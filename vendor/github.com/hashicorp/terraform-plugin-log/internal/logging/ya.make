GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    filtering.go
    log.go
    options.go
    provider.go
    sdk.go
    sink.go
)

GO_XTEST_SRCS(
    filtering_test.go
    options_test.go
)

END()

RECURSE(
    gotest
)
