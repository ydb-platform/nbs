GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    backoff.go
    compare.go
    driver.go
    file_line.go
    query_bind.go
    session.go
    time.go
    topic.go
)

GO_TEST_SRCS(compare_test.go)

END()

RECURSE(
    gotest
)
