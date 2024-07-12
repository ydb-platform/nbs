GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    quota_controller_client.go
    service_controller_client.go
    version.go
)

GO_XTEST_SRCS(
    quota_controller_client_example_test.go
    service_controller_client_example_test.go
)

END()

RECURSE(
    gotest
    servicecontrolpb
)
