GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    constants.go
    error.go
    kdf.go
    structures.go
    tpm2.go
)

IF (OS_LINUX)
    SRCS(
        open_other.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        open_other.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        open_windows.go
    )
ENDIF()

END()

RECURSE(
    credactivation
)
