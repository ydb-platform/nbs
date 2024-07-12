GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    api_a_bit_of_everything_service.go
    api_camel_case_service_name.go
    api_echo_rpc.go
    client.go
    configuration.go
    enum_helper.go
    model_a_bit_of_everything_nested.go
    model_examplepb_a_bit_of_everything.go
    model_examplepb_a_bit_of_everything_repeated.go
    model_examplepb_body.go
    model_examplepb_book.go
    model_examplepb_error_object.go
    model_examplepb_error_response.go
    model_examplepb_numeric_enum.go
    model_examplepb_update_v2_request.go
    model_message_path_enum_nested_path_enum.go
    model_nested_deep_enum.go
    model_pathenum_path_enum.go
    model_protobuf_any.go
    model_protobuf_field_mask.go
    model_runtime_error.go
    model_sub_string_message.go
    response.go
)

END()
