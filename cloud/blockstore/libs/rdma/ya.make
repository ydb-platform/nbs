RECURSE(
    iface
)

IF (NOT OPENSOURCE)
    RECURSE(
        impl
    )
ENDIF()
