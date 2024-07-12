GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

IF (OS_DARWIN)
    SRCS(
        bsd_bpf_sniffer.go
    )
ENDIF()

END()
