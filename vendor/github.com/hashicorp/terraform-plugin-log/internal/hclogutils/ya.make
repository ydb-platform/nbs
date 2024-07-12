GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    args.go
    logger_options.go
)

GO_XTEST_SRCS(args_test.go)

END()

RECURSE(
    gotest
)
