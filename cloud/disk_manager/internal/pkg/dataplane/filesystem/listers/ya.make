GO_LIBRARY()

SRCS(
    lister.go
    filestore_lister.go
)

END()

RECURSE(
    mocks
)
