FUZZ()

OWNER(g:cloud-nbs)

SIZE(LARGE)

TAG(
    ya:fat
    ya:not_autocheck
    ya:manual
)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/daemon/local
    cloud/blockstore/libs/service
    cloud/blockstore/tests/fuzzing/common
)

END()
