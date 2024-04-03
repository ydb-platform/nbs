GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    fake_mounter.go
    mount_helper_common.go
    mount_helper_unix.go
    # mount_helper_windows.go
    mount_linux.go
    # mount_unsupported.go
    # mount_windows.go
    mount.go
    resizefs_linux.go
    # resizefs_unsupported.go
)

END()
