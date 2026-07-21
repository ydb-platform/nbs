PY3TEST()

    PY_SRCS (
        conftest.py
    )

    FORK_TESTS()

    TEST_SRCS(
        test_alter_tiering.py
        test_insert.py
        test_read_update_write_load.py
        test_scheme_load.py
        test_simple.py
    )

    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
    DEPENDS(
        )

    PEERDIR(
        contrib/python/Flask
        contrib/python/Flask-Cors
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/boto3
        contrib/python/moto
        contrib/python/requests
        library/python/port_manager
        library/python/testing/yatest_common
        library/recipes/common
        contrib/ydb/public/sdk/python
        contrib/ydb/public/sdk/python/enable_v3_new_behavior
        contrib/ydb/tests/library
        contrib/ydb/tests/olap/common
        contrib/ydb/tests/olap/lib
        contrib/ydb/tests/olap/scenario/helpers
    )

    SIZE(MEDIUM)
    IF (SANITIZER_TYPE)
        REQUIREMENTS(cpu:4)
    ELSE()
        REQUIREMENTS(cpu:2)
    ENDIF()

END()
