GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    type.go
    value.go
    value_state.go
)

END()

RECURSE(
    xattr
)
