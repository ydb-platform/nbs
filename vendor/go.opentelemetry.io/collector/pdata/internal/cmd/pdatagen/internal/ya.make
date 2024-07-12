GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    base_fields.go
    base_slices.go
    base_structs.go
    packages.go
    pcommon_package.go
    plog_package.go
    plogotlp_package.go
    pmetric_package.go
    pmetricotlp_package.go
    primitive_slice_structs.go
    ptrace_package.go
    ptraceotlp_package.go
)

END()
