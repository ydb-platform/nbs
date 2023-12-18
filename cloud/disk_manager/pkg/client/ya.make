GO_LIBRARY()

SRCS(
    client.go
    interface.go
)

END()

RECURSE(
    codes
    config
)
