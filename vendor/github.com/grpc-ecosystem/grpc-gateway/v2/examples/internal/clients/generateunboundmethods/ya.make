GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    api_generate_unbound_methods_echo_service.go
    client.go
    configuration.go
    model_examplepb_generate_unbound_methods_simple_message.go
    model_protobuf_any.go
    model_rpc_status.go
    model_runtime_error.go
    response.go
)

END()
