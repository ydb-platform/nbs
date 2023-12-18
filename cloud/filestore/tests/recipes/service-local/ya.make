IF (SANITIZER_TYPE AND NOT OPENSOURCE)
    PACKAGE()

    FROM_SANDBOX(
        5529761290
        AUTOUPDATED service_local_recipe
        EXECUTABLE
        OUT service-local-recipe
    )

    END()
ELSE()
    PY3_PROGRAM(service-local-recipe)

    PY_SRCS(__main__.py)

    PEERDIR(
        cloud/filestore/tests/python/lib

        library/python/testing/recipe
        library/python/testing/yatest_common
    )

    END()
ENDIF()
