GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    byteview.go
    groupcache.go
    http.go
    peers.go
    sinks.go
)

GO_TEST_SRCS(
    byteview_test.go
    groupcache_test.go
    http_test.go
)

END()

RECURSE(
    consistenthash
    gotest
    groupcachepb
    lru
    singleflight
    testpb
)
