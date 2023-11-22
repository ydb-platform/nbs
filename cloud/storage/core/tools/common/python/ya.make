PY3_LIBRARY()

PY_SRCS(
    core_pattern.py
    daemon.py
)

IF (NOT OPENSOURCE)
    PEERDIR(
        contrib/python/requests     # TODO: NBS-4453
    )
ENDIF()

END()
