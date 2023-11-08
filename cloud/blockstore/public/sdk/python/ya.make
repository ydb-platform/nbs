RECURSE(
    protos
)

IF (NOT OPENSOURCE)
    RECURSE(
        client  # TODO(NBS-4409): add to opensource
    )
ENDIF()
