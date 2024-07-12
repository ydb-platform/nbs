GO_TEST()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

GO_TEST_SRCS(
    custom_global_function_test.go
    custom_instance_function_test.go
    simple_test.go
)

END()
