GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    a_bit_of_everything.pb.go
    a_bit_of_everything.pb.gw.go
    echo_service.pb.go
    echo_service.pb.gw.go
    flow_combination.pb.go
    flow_combination.pb.gw.go
    generate_unbound_methods.pb.go
    generate_unbound_methods.pb.gw.go
    non_standard_names.pb.go
    non_standard_names.pb.gw.go
    response_body_service.pb.go
    response_body_service.pb.gw.go
    stream.pb.go
    stream.pb.gw.go
    unannotated_echo_service.pb.go
    unannotated_echo_service.pb.gw.go
    use_go_template.pb.go
    use_go_template.pb.gw.go
    wrappers.pb.go
    wrappers.pb.gw.go
)

END()
