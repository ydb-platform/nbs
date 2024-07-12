GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    cache.go
    fs.go
    ro.go
)

GO_TEST_SRCS(
    cache_test.go
    fs_test.go
    ro_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
