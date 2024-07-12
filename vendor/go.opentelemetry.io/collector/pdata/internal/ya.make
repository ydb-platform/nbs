GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    generated_wrapper_byteslice.go
    generated_wrapper_float64slice.go
    generated_wrapper_instrumentationscope.go
    generated_wrapper_resource.go
    generated_wrapper_uint64slice.go
    wrapper_logs.go
    wrapper_map.go
    wrapper_metrics.go
    wrapper_slice.go
    wrapper_traces.go
    wrapper_tracestate.go
    wrapper_value.go
)

END()

RECURSE(
    cmd
    data
    json
    otlp
)
