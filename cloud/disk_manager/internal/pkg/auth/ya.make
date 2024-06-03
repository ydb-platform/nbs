GO_LIBRARY()

SRCS(
    credentials.go
)

END()

RECURSE(
    config
    oauth2_jwt
)
