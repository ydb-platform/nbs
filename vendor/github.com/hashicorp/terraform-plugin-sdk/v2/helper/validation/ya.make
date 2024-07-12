GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    float.go
    int.go
    list.go
    map.go
    meta.go
    network.go
    strings.go
    testing.go
    time.go
    uuid.go
    web.go
)

GO_TEST_SRCS(
    float_test.go
    int_test.go
    list_test.go
    map_test.go
    meta_test.go
    network_test.go
    strings_test.go
    time_test.go
    uuid_test.go
    web_test.go
)

END()

RECURSE(
    gotest
)
