GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    function_id.go
    record.go
)

GO_TEST_SRCS(
    function_id_test.go
    record_test.go
)

END()

RECURSE(
    gotest
)
