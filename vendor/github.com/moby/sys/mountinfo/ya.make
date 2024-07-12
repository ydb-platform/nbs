GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_SKIP_TESTS(TestMountedBy)

SRCS(
    doc.go
    mountinfo.go
    mountinfo_filters.go
)

IF (OS_LINUX)
    SRCS(
        mounted_linux.go
        mounted_unix.go
        mountinfo_linux.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        mounted_unix.go
        mountinfo_bsd.go
        mountinfo_freebsdlike.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        mountinfo_windows.go
    )
ENDIF()

END()
