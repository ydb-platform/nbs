GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    options.go
    textlogger.go
    textlogger_slog.go
)

GO_XTEST_SRCS(
    example_test.go
    output_test.go
    textlogger_slog_test.go
    textlogger_test.go
)

END()

RECURSE(
    gotest
)
