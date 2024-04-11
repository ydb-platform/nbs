GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    contextual_slog.go
    contextual.go
    exit.go
    format.go
    imports.go
    k8s_references_slog.go
    k8s_references.go
    klog_file_others.go
    # klog_file_windows.go
    klog_file.go
    klog.go
    klogr_slog.go
    klogr.go
    safeptr.go
)

END()

RECURSE(
    internal
)
