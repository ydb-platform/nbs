RECURSE(
    breakpad
    common
    analytics
    ops
    testing
)

IF (NOT OPENSOURCE)
    RECURSE(
        bot     # NBS-4409
    )
ENDIF()
