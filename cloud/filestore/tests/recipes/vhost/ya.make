IF (SANITIZER_TYPE AND NOT OPENSOURCE)
    PACKAGE()

    FROM_SANDBOX(
        5529761343
        AUTOUPDATED vhost_recipe
        EXECUTABLE
        OUT vhost-recipe
    )

    END()
ELSE()
    PY3_PROGRAM(vhost-recipe)

    PY_SRCS(__main__.py)

    PEERDIR(
        cloud/filestore/tests/python/lib

        library/python/testing/recipe
        library/python/testing/yatest_common
    )

    DEPENDS(
        cloud/filestore/apps/vhost
    )

    END()
ENDIF()
