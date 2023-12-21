PY3_PROGRAM(accessservice-mock)

PY_SRCS(
    __main__.py
    mock_service.py
    control_service.py
)

PEERDIR(
    ydb/public/api/client/yc_private/servicecontrol

    contrib/python/Flask
)

END()
