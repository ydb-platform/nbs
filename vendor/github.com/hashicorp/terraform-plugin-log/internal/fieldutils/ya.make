GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    field_maps.go
)

GO_XTEST_SRCS(field_maps_test.go)

END()

RECURSE(
    gotest
)
