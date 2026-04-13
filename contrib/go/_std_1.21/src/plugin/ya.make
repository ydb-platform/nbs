GO_LIBRARY()

NO_COMPILER_WARNINGS()

IF (CGO_ENABLED)
    IF (OS_DARWIN)
        CGO_LDFLAGS(-ldl)
    ENDIF()

    IF (OS_LINUX)
        CGO_LDFLAGS(-ldl)
    ENDIF()
ELSE()
    IF (OS_DARWIN OR OS_LINUX)
        SRCS(
            plugin_stubs.go
        )
    ENDIF()
ENDIF()

SRCS(
    plugin.go
)

GO_XTEST_SRCS(plugin_test.go)

IF (OS_LINUX AND CGO_ENABLED)
    CGO_SRCS(plugin_dlopen.go)
ENDIF()

IF (OS_DARWIN AND CGO_ENABLED)
    CGO_SRCS(plugin_dlopen.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        plugin_stubs.go
    )
ENDIF()

END()

RECURSE(
)
