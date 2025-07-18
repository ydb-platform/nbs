GO_LIBRARY()

SRCS(
    marshal.go
    stringset.go
    util.go
)

END()

RECURSE(
    protos
)
