GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-2-Clause)

SRCS(
    browser.go
)

GO_TEST_SRCS(example_test.go)

IF (OS_LINUX)
    SRCS(
        browser_linux.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        browser_darwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        browser_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
