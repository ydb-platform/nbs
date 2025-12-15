PY3_LIBRARY()

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
ENDIF()

IF (OPENSOURCE)
    # YQ-3351: enabling python style checks only for opensource
    STYLE_PYTHON()
ENDIF()

PY_SRCS(
    base.py
    select_missing_database.py
    select_missing_table.py
    select_positive_common.py
)

PEERDIR(
    contrib/ydb/library/yql/providers/generic/connector/api/common
    contrib/ydb/library/yql/providers/generic/connector/tests/utils
    contrib/ydb/library/yql/providers/generic/connector/api/service/protos
    contrib/ydb/public/api/protos
)

END()
