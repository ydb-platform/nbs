UNITTEST_FOR(cloud/blockstore/libs/nvme)

IF (NOT GITHUB_CI)
    # To run these tests, nvme devices are required. In github CI VM, such devices are emulated via nvme-loop.
    # TODO(sharpeye): add the option to run tests locally
    TAG(ya:manual)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    nvme_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/nvme/testing
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/nvme-loop/recipe.inc)

END()
