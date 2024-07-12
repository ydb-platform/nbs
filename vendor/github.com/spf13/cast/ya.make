GO_LIBRARY()

SUBSCRIBER(
    g:go-contrib
    g:marketsre
)

LICENSE(MIT)

SRCS(
    cast.go
    caste.go
    timeformattype_string.go
)

GO_TEST_SRCS(cast_test.go)

END()

RECURSE(
    gotest
)
