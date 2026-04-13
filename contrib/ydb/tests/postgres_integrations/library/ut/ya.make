PY3TEST()

TEST_SRCS(
    integrations_test.py
)


DATA(
    arcadia/contrib/ydb/tests/postgres_integrations/library/ut/data
)

PEERDIR(
    contrib/ydb/tests/postgres_integrations/library
)

END()
