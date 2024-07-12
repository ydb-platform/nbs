GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    encoding.go
    generated_logrecord.go
    generated_logrecordslice.go
    generated_resourcelogs.go
    generated_resourcelogsslice.go
    generated_scopelogs.go
    generated_scopelogsslice.go
    json.go
    log_record_flags.go
    logs.go
    pb.go
    severity_number.go
)

GO_TEST_SRCS(
    generated_logrecord_test.go
    generated_logrecordslice_test.go
    generated_resourcelogs_test.go
    generated_resourcelogsslice_test.go
    generated_scopelogs_test.go
    generated_scopelogsslice_test.go
    json_test.go
    log_record_flags_test.go
    logs_test.go
    pb_test.go
    severity_number_test.go
)

END()

RECURSE(
    gotest
    plogotlp
)
