PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    base.py
    collection.py
    join.py
    select_datetime.py
    select_missing_database.py
    select_missing_table.py
    select_positive_clickhouse.py
    select_positive_common.py
    select_positive_postgresql.py
    select_positive_postgresql_schema.py
)

PEERDIR(
    contrib/ydb/library/yql/providers/generic/connector/api/common
    contrib/ydb/library/yql/providers/generic/connector/tests/utils
    contrib/ydb/library/yql/providers/generic/connector/api/service/protos
    contrib/ydb/public/api/protos
)

END()
