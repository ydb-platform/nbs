GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    batch.go
    control.go
    dgramopt.go
    doc.go
    endpoint.go
    genericopt.go
    header.go
    helper.go
    iana.go
    icmp.go
    packet.go
    payload.go
    sockopt.go
    sockopt_posix.go
)

IF (OS_LINUX)
    SRCS(
        control_pktinfo.go
        control_unix.go
        icmp_linux.go
        payload_cmsg.go
        sys_asmreq_stub.go
        sys_asmreqn.go
        sys_bpf.go
        sys_linux.go
        sys_ssmreq.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        zsys_linux_amd64.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        zsys_linux_arm64.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        control_bsd.go
        control_pktinfo.go
        control_unix.go
        icmp_stub.go
        payload_cmsg.go
        sys_asmreq.go
        sys_asmreqn.go
        sys_bpf_stub.go
        sys_darwin.go
        sys_ssmreq.go
        zsys_darwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        control_windows.go
        icmp_stub.go
        payload_nocmsg.go
        sys_asmreq.go
        sys_asmreqn_stub.go
        sys_bpf_stub.go
        sys_ssmreq_stub.go
        sys_windows.go
    )
ENDIF()

END()
