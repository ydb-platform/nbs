GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    compound_packet.go
    doc.go
    errors.go
    extended_report.go
    full_intra_request.go
    goodbye.go
    header.go
    packet.go
    packet_buffer.go
    packet_stringifier.go
    picture_loss_indication.go
    rapid_resynchronization_request.go
    raw_packet.go
    receiver_estimated_maximum_bitrate.go
    receiver_report.go
    reception_report.go
    rfc8888.go
    sender_report.go
    slice_loss_indication.go
    source_description.go
    transport_layer_cc.go
    transport_layer_nack.go
    util.go
)

GO_TEST_SRCS(
    compound_packet_test.go
    extended_report_test.go
    full_intra_request_test.go
    fuzz_test.go
    goodbye_test.go
    header_test.go
    packet_buffer_test.go
    packet_stringifier_test.go
    packet_test.go
    picture_loss_indication_test.go
    rapid_resynchronization_request_test.go
    raw_packet_test.go
    receiver_estimated_maximum_bitrate_test.go
    receiver_report_test.go
    rfc8888_test.go
    sender_report_test.go
    slice_loss_indication_test.go
    source_description_test.go
    transport_layer_cc_test.go
    transport_layer_nack_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
