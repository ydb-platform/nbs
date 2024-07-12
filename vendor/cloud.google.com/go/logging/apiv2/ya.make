GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auxiliary.go
    config_client.go
    doc.go
    info.go
    logging_client.go
    metrics_client.go
    path_funcs.go
    version.go
)

GO_TEST_SRCS(WriteLogEntries_smoke_test.go)

GO_XTEST_SRCS(
    config_client_example_test.go
    logging_client_example_test.go
    metrics_client_example_test.go
)

END()

RECURSE(
    gotest
    loggingpb
)
