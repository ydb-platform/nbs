IF (SANITIZER_TYPE AND NOT OPENSOURCE)
    PACKAGE()

    FROM_SANDBOX(
        5529761270
        AUTOUPDATED vhost_endpoint_recipe
        EXECUTABLE
        OUT vhost-endpoint-recipe
    )

    END()
ELSE()
    PY3_PROGRAM(vhost-endpoint-recipe)

    PY_SRCS(__main__.py)

    PEERDIR(
        cloud/filestore/tests/python/lib

        library/python/testing/recipe
        library/python/testing/yatest_common
    )

    END()
ENDIF()
