PY3_PROGRAM(storage-unstable-process)

IF (NOT OPENSOURCE)
    PEERDIR(
        contrib/python/requests     # TODO: NBS-4453
    )
ENDIF()

PY_SRCS(
    __main__.py
)

END()
