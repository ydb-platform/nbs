GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    service_manager_client.go
    version.go
)

GO_XTEST_SRCS(service_manager_client_example_test.go)

END()

RECURSE(
    gotest
    servicemanagementpb
)
