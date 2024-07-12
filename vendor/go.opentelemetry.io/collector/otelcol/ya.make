GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    collector.go
    command.go
    command_components.go
    command_validate.go
    config.go
    configprovider.go
    factories.go
    flags.go
    unmarshaler.go
)

GO_TEST_SRCS(
    collector_test.go
    command_components_test.go
    command_test.go
    command_validate_test.go
    config_test.go
    configprovider_test.go
    factories_test.go
    flags_test.go
    unmarshaler_test.go
)

IF (OS_WINDOWS)
    SRCS(
        collector_windows.go
    )

    GO_TEST_SRCS(collector_windows_test.go)
ENDIF()

END()

RECURSE(
    gotest
    internal
    otelcoltest
)
