RECURSE(
    iface
)

IF (NOT OPENSOURCE)
    RECURSE(
        pqimpl
    )
ENDIF()
