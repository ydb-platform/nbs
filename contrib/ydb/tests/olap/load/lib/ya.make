PY3_LIBRARY()

    PY_SRCS (
        conftest.py
        clickbench.py
        tpcds.py
        tpch.py
    )

    PEERDIR (
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/ydb/public/sdk/python/enable_v3_new_behavior
        contrib/ydb/tests/olap/lib
        contrib/ydb/tests/olap/scenario/helpers
        library/python/testing/yatest_common
        contrib/ydb/public/sdk/python
    )

END()
