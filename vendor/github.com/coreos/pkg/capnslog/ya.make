GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    formatters.go
    glog_formatter.go
    log_hijack.go
    logmap.go
    pkg_logger.go
)

IF (OS_LINUX)
    SRCS(
        init.go
        journald_formatter.go
        syslog_formatter.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        init.go
        journald_formatter.go
        syslog_formatter.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        init_windows.go
    )
ENDIF()

END()
