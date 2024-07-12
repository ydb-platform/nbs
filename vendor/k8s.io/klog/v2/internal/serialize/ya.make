GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    keyvalues.go
    keyvalues_slog.go
)

GO_XTEST_SRCS(keyvalues_test.go)

END()

RECURSE(
    gotest
)
