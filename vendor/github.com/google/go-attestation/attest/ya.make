GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    activation.go
    application_key.go
    attest.go
    certification.go
    challenge.go
    eventlog.go
    eventlog_workarounds.go
    secureboot.go
    storage.go
    tpm.go
    tpm_fake.go
    vendors.go
    win_events.go
    wrapped_tpm20.go
)

IF (OS_LINUX)
    SRCS(
        tpm_linux.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        tpm_other.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        key_windows.go
        pcp_windows.go
        tpm_windows.go
    )
ENDIF()

END()

RECURSE(
    attest-tool
    internal
)
