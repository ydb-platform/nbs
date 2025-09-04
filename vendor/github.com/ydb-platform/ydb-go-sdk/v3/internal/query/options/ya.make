GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    execute.go
    execute_script.go
    retry.go
)

GO_TEST_SRCS(execute_test.go)

END()

RECURSE(
    gotest
)
