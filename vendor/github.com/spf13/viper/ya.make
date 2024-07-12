GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    flags.go
    logger.go
    util.go
    viper.go
    viper_go1_15.go
    watch.go
)

GO_TEST_SRCS(
    flags_test.go
    overrides_test.go
    util_test.go
    viper_test.go
    viper_yaml_test.go
)

END()

RECURSE(
    gotest
    internal
    # remote
)
