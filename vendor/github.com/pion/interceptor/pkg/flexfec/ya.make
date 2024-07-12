GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    flexfec_coverage.go
    flexfec_encoder.go
)

END()

RECURSE(
    util
)
