PY3_LIBRARY()

PY_SRCS(
    __init__.py
    http_env.py
    mock_nc_iam.py
    mvp_service.py
)

PEERDIR(
    contrib/python/requests
    contrib/ydb/tests/library
)

END()
