GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    api_response_body_service.go
    client.go
    configuration.go
    model_examplepb_repeated_response_body_out.go
    model_examplepb_repeated_response_body_out_response.go
    model_examplepb_repeated_response_strings.go
    model_examplepb_response_body_out.go
    model_examplepb_response_body_out_response.go
    model_protobuf_any.go
    model_response_response_type.go
    model_runtime_error.go
    model_runtime_stream_error.go
    model_stream_result_of_examplepb_response_body_out.go
    response.go
)

END()
