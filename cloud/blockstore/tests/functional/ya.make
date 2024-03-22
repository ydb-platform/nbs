RECURSE_ROOT_RELATIVE(
    cloud/storage/core/libs

    cloud/blockstore/apps/client/lib
    cloud/blockstore/libs
    cloud/blockstore/tests/client
)

IF(NOT SANITIZER_TYPE)
    RECURSE_ROOT_RELATIVE(
        cloud/blockstore/public/sdk/go
    )
ENDIF()

# DEVTOOLSSUPPORT-18977
IF (SANITIZER_TYPE != "undefined" AND SANITIZER_TYPE != "memory")
    RECURSE_ROOT_RELATIVE(
        cloud/blockstore/tests/plugin
    )
ENDIF()

IF (SANITIZER_TYPE != "thread")
    RECURSE_ROOT_RELATIVE(
        cloud/blockstore/public/sdk/python
    )
ENDIF()
