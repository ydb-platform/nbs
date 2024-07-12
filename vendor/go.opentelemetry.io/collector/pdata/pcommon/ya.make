GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    generated_byteslice.go
    generated_float64slice.go
    generated_instrumentationscope.go
    generated_resource.go
    generated_uint64slice.go
    map.go
    slice.go
    spanid.go
    timestamp.go
    trace_state.go
    traceid.go
    value.go
)

GO_TEST_SRCS(
    generated_byteslice_test.go
    generated_float64slice_test.go
    generated_instrumentationscope_test.go
    generated_resource_test.go
    generated_uint64slice_test.go
    map_test.go
    slice_test.go
    spanid_test.go
    timestamp_test.go
    trace_state_test.go
    traceid_test.go
    value_test.go
)

END()

RECURSE(
    gotest
)
