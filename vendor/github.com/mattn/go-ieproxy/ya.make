GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    ieproxy.go
    proxy_middleman.go
    utils.go
)

GO_TEST_SRCS(
    example_test.go
    utils_test.go
)

IF (OS_LINUX)
    SRCS(
        ieproxy_unix.go
        pac_unix.go
        proxy_middleman_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        ieproxy_unix.go
        pac_unix.go
        proxy_middleman_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        ieproxy_windows.go
        kernel32_data_windows.go
        pac_windows.go
        proxy_middleman_windows.go
        winhttp_data_windows.go
    )

    GO_TEST_SRCS(windows_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
