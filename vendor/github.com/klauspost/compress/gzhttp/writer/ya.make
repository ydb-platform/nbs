GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(
    interface.go
)

END()

RECURSE(
    gzkp
)
