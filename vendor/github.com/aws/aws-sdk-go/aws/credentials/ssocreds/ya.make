GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    provider.go
)

GO_TEST_SRCS(provider_test.go)

IF (OS_LINUX)
    SRCS(os.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(os.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(os_windows.go)
ENDIF()

END()

RECURSE(gotest)
