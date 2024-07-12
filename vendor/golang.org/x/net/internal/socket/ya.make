GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    norace.go
    rawconn.go
    rawconn_msg.go
    socket.go
    sys.go
    sys_posix.go
)

IF (OS_LINUX)
    SRCS(
        cmsghdr.go
        cmsghdr_linux_64bit.go
        cmsghdr_unix.go
        complete_dontwait.go
        error_unix.go
        iovec_64bit.go
        mmsghdr_unix.go
        msghdr_linux.go
        msghdr_linux_64bit.go
        rawconn_mmsg.go
        sys_const_unix.go
        sys_linux.go
        sys_unix.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        sys_linux_amd64.go
        zsys_linux_amd64.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        sys_linux_arm64.go
        zsys_linux_arm64.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        cmsghdr.go
        cmsghdr_bsd.go
        cmsghdr_unix.go
        complete_dontwait.go
        empty.s
        error_unix.go
        iovec_64bit.go
        mmsghdr_stub.go
        msghdr_bsd.go
        msghdr_bsdvar.go
        rawconn_nommsg.go
        sys_bsd.go
        sys_const_unix.go
        sys_unix.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
        zsys_darwin_amd64.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        zsys_darwin_arm64.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        cmsghdr_stub.go
        complete_nodontwait.go
        error_windows.go
        iovec_stub.go
        mmsghdr_stub.go
        msghdr_stub.go
        rawconn_nommsg.go
        sys_windows.go
    )
ENDIF()

END()
