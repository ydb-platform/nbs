GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    2q.go
    doc.go
    lru.go
)

GO_TEST_SRCS(
    2q_test.go
    lru_test.go
    testing_test.go
)

END()

RECURSE(
    expirable
    gotest
    internal
    simplelru
)
