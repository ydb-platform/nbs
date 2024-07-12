GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    noncolorable.go
)

GO_TEST_SRCS(colorable_test.go)

IF (OS_LINUX)
    SRCS(
        colorable_others.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        colorable_others.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        colorable_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
