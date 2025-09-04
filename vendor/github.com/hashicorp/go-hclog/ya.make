GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.6.3)

GO_SKIP_TESTS(TestInterceptLogger)

SRCS(
    context.go
    exclude.go
    global.go
    interceptlogger.go
    intlogger.go
    logger.go
    nulllogger.go
    stacktrace.go
    stdlog.go
    writer.go
)

GO_TEST_SRCS(
    context_test.go
    exclude_test.go
    interceptlogger_test.go
    logger_loc_test.go
    logger_test.go
    nulllogger_test.go
    stdlog_test.go
)

IF (OS_LINUX)
    SRCS(
        colorize_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        colorize_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        colorize_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
