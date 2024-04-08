GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    mounted_linux.go
    mounted_unix.go
    # mountinfo_bsd.go
    mountinfo_filters.go
    # mountinfo_freebsdlike.go
    mountinfo.go
    mountinfo_linux.go
    # mountinfo_openbsd.go
    # mountinfo_unsupported.go
    # mountinfo_windows.go
)

END()
