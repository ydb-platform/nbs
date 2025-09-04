GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.16.0)

SRCS(
    grpc_api_configuration.go
    grpc_api_service.go
    registry.go
    services.go
    types.go
)

GO_TEST_SRCS(
    grpc_api_configuration_test.go
    registry_test.go
    services_test.go
    types_test.go
)

END()

RECURSE(
    gotest
)
