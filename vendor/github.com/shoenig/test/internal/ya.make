SUBSCRIBER(g:go-contrib)

RECURSE(
    assertions
    constraints
    util
)

IF (OS_LINUX)
    RECURSE(
        brokenfs
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        brokenfs
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        brokenfs
    )
ENDIF()
