IF (SANITIZER_TYPE AND NOT OPENSOURCE)
    PACKAGE()

    FROM_SANDBOX(
        5559438185
        AUTOUPDATED service_kikimr_recipe
        EXECUTABLE
        OUT service-kikimr-recipe
    )

    END()
ELSE()
    PY3_PROGRAM(service-kikimr-recipe)

    PY_SRCS(__main__.py)

    PEERDIR(
        cloud/filestore/tests/python/lib

        library/python/testing/recipe
        library/python/testing/yatest_common
    )

    END()
ENDIF()
