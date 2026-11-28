UNITTEST()

SIZE(MEDIUM)
FORK_SUBTESTS()

SRCS(
    plugin_lifecycle_ut.cpp
)

PEERDIR(
    cloud/vm/api
    library/cpp/testing/common
)

DEPENDS(
    cloud/vm/blockstore
)

END()
