GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    function_id.go
    record.go
)

GO_TEST_SRCS(record_test.go)

END()

RECURSE(
    gotest
)
