GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    2q.go
    arc.go
    doc.go
    lru.go
    testing.go
)

GO_TEST_SRCS(
    2q_test.go
    arc_test.go
    lru_test.go
)

END()

RECURSE(
    gotest
    simplelru
)
