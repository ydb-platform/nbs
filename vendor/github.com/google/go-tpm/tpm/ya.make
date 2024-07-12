GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    commands.go
    constants.go
    errors.go
    pcrs.go
    structures.go
    tpm.go
    verify.go
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
