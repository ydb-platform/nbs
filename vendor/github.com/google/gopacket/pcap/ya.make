GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

ADDINCL(contrib/libs/libpcap)

PEERDIR(contrib/libs/libpcap)

SRCS(
    doc.go
    pcap.go
)

GO_TEST_SRCS(
    bpf_test.go
    pcap_test.go
    pcapgo_test.go
    pcapnggo_test.go
)

IF (OS_LINUX AND CGO_ENABLED)
    CGO_SRCS(pcap_unix.go)
ENDIF()

IF (OS_DARWIN AND CGO_ENABLED)
    CGO_SRCS(pcap_unix.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        pcap_windows.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
        defs_windows_amd64.go
    )
ENDIF()

END()

RECURSE(
    gopacket_benchmark
    # gotest
)
