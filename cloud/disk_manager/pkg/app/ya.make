GO_LIBRARY()

SRCS(
    controlplane.go
    dataplane.go
    export.go
    run.go
)

GO_TEST_SRCS(
    export_test.go
)

END()
