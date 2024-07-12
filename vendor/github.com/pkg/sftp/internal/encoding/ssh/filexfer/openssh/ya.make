GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-2-Clause)

SRCS(
    fsync.go
    hardlink.go
    openssh.go
    posix-rename.go
    statvfs.go
)

GO_TEST_SRCS(
    fsync_test.go
    hardlink_test.go
    posix-rename_test.go
    statvfs_test.go
)

END()

RECURSE(
    gotest
)
