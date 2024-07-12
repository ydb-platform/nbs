GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    diff.go
    instancetype.go
    instancetype_string.go
    resource.go
    resource_address.go
    resource_mode.go
    resource_mode_string.go
    resource_provider.go
    schemas.go
    state.go
    state_filter.go
    util.go
)

GO_TEST_SRCS(
    diff_test.go
    resource_address_test.go
    resource_test.go
    state_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
