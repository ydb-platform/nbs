GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    cache.go
    endpoint.go
    sync_map.go
)

GO_TEST_SRCS(
    cache_test.go
    endpoint_test.go
    sync_map_test.go
)

END()

RECURSE(
    gotest
)
