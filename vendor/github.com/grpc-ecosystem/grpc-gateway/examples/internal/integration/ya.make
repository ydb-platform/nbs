GO_TEST()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

GO_XTEST_SRCS(
    client_test.go
    fieldmask_test.go
    integration_test.go
    main_test.go
    proto_error_test.go
)

END()
