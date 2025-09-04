GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.7.0)

SRCS(
    cache.go
    env.go
    env_generic_sugar.go
    interface.go
    maintest.go
    scope_info.go
)

GO_TEST_SRCS(
    cache_test.go
    env_generic_sugar_test.go
    env_test.go
    interface_test.go
    maintest_test.go
    scope_info_test.go
    test_helpers_test.go
)

END()

RECURSE(
    gotest
    internal
    sf
)
