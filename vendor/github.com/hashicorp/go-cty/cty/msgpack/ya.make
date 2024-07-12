GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    doc.go
    dynamic.go
    infinity.go
    marshal.go
    type_implied.go
    unknown.go
    unmarshal.go
)

GO_TEST_SRCS(
    roundtrip_test.go
    type_implied_test.go
)

END()

RECURSE(
    gotest
)
