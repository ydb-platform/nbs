PY3TEST()

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server
    cloud/blockstore/apps/disk_agent

    cloud/filestore/apps/server

    cloud/disk_manager/cmd/disk-manager
    cloud/disk_manager/cmd/disk-manager-init-db
    cloud/disk_manager/cmd/disk-manager-admin
    cloud/disk_manager/test/mocks/metadata

    cloud/tasks/test/nemesis

    contrib/python/moto/bin
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

PEERDIR(
    cloud/disk_manager/test/recipe
    cloud/tasks/test/common
    contrib/ydb/tests/library
    library/python/testing/recipe
)

SIZE(LARGE)
TIMEOUT(3600)

# use ya:not_autocheck and ya:manual with sanitizers until we have sufficient
# quota
IF (SANITIZER_TYPE)
    TAG(
        ya:fat
        ya:force_sandbox
        ya:not_autocheck
        ya:manual
        sb:ttl=3
        sb:logs_ttl=3
    )
ELSE()
    TAG(
        ya:fat
        ya:force_sandbox
        sb:ttl=3
        sb:logs_ttl=3
    )
ENDIF()

IF (OPENSOURCE)
    DEPENDS(
        cloud/storage/core/tools/testing/ydb/bin
    )
ELSE()
    DEPENDS(
        contrib/ydb/apps/ydbd
    )
ENDIF()


FORK_SUBTESTS()
SPLIT_FACTOR(8)

REQUIREMENTS(
    cpu:8
    ram:30
)

END()
