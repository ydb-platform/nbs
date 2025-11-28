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
    dqrun.py
    kqprun.py
    parent.py
    result.py
    runners.py
)

PEERDIR(
    contrib/python/Jinja2
    contrib/python/PyYAML
    contrib/ydb/library/yql/providers/generic/connector/api/common
    contrib/ydb/library/yql/providers/generic/connector/api/service/protos
    contrib/ydb/library/yql/providers/generic/connector/tests/utils
    contrib/ydb/public/api/protos
    yt/python/yt/yson
)

END()
