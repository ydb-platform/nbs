IF (SANITIZER_TYPE AND NOT OPENSOURCE)
    PACKAGE()

    FROM_SANDBOX(
        5529761469
        AUTOUPDATED mount_recipe
        EXECUTABLE
        OUT mount-recipe
    )

    END()
ELSE()
    PY3_PROGRAM(mount-recipe)

    PY_SRCS(__main__.py)

    PEERDIR(
        cloud/filestore/tests/python/lib

        library/python/testing/recipe
        library/python/testing/yatest_common
    )

    END()
ENDIF()
