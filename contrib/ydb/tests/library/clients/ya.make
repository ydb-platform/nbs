PY23_LIBRARY()

PY_SRCS(
    __init__.py
    kikimr_bridge_client.py
    kikimr_client.py
    kikimr_config_client.py
    kikimr_dynconfig_client.py
    kikimr_http_client.py
    kikimr_keyvalue_client.py
    kikimr_monitoring.py
    kikimr_scheme_client.py
)

PEERDIR(
    contrib/ydb/core/protos
    # ydb/tests/library  # TODO: remove dependency, commented because of loop. Needed because protobuf_ss
    contrib/ydb/public/api/protos
    contrib/ydb/public/api/grpc
)

END()
