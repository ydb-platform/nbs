RECURSE(
    common
)

IF (NOT OPENSOURCE)
    RECURSE(
        fuzzing-null    # TODO(NBS-4409): add to opensource
        fuzzing-ydb     # TODO(NBS-4409): add to opensource
    )
ENDIF()
