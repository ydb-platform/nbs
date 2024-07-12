GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    arrival_time_map.go
    header_extension_interceptor.go
    sender_interceptor.go
    twcc.go
)

GO_TEST_SRCS(
    arrival_time_map_test.go
    header_extension_interceptor_test.go
    sender_interceptor_test.go
    twcc_test.go
)

END()

RECURSE(
    gotest
)
