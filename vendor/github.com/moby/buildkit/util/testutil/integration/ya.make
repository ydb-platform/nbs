GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    azurite.go
    containerd.go
    dockerd.go
    frombinary.go
    minio.go
    oci.go
    pins.go
    registry.go
    run.go
    sandbox.go
    util.go
)

IF (OS_LINUX)
    SRCS(
        sandbox_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        sandbox_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        sandbox_windows.go
    )
ENDIF()

END()
