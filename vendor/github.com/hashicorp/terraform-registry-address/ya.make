GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    errors.go
    module.go
    module_package.go
    provider.go
)

GO_TEST_SRCS(
    module_test.go
    provider_test.go
)

END()

RECURSE(
    gotest
)
