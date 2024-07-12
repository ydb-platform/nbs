GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    freetype.go
)

GO_TEST_SRCS(freetype_test.go)

END()

RECURSE(
    gotest
    raster
    truetype
)
