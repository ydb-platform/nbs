GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    encoding.go
    generated_resourcespans.go
    generated_resourcespansslice.go
    generated_scopespans.go
    generated_scopespansslice.go
    generated_span.go
    generated_spanevent.go
    generated_spaneventslice.go
    generated_spanlink.go
    generated_spanlinkslice.go
    generated_spanslice.go
    generated_status.go
    json.go
    pb.go
    span_kind.go
    status_code.go
    traces.go
)

GO_TEST_SRCS(
    generated_resourcespans_test.go
    generated_resourcespansslice_test.go
    generated_scopespans_test.go
    generated_scopespansslice_test.go
    generated_span_test.go
    generated_spanevent_test.go
    generated_spaneventslice_test.go
    generated_spanlink_test.go
    generated_spanlinkslice_test.go
    generated_spanslice_test.go
    generated_status_test.go
    json_test.go
    pb_test.go
    span_kind_test.go
    status_code_test.go
    traces_test.go
)

END()

RECURSE(
    gotest
    ptraceotlp
)
