LIBRARY()

SRCS(
    nvme.cpp
    nvme_stub.cpp
)

IF(OS_LINUX)
    SRCS(
        nvme_linux.cpp
    )
ENDIF(OS_LINUX)

PEERDIR(
    cloud/blockstore/config
)

END()

RECURSE_FOR_TESTS(
    ut_nvme
)

RECURSE(testing)
