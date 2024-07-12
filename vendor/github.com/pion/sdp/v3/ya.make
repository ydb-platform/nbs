GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    base_lexer.go
    common_description.go
    direction.go
    extmap.go
    jsep.go
    marshal.go
    media_description.go
    sdp.go
    session_description.go
    time_description.go
    unmarshal.go
    unmarshal_cache.go
    util.go
)

GO_TEST_SRCS(
    base_lexer_test.go
    direction_test.go
    extmap_test.go
    fuzz_test.go
    marshal_test.go
    media_description_test.go
    sessiondescription_test.go
    unmarshal_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
