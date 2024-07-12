SUBSCRIBER(g:go-contrib)

RECURSE(
    apicaps
    appcontext
    attestation
    bklog
    contentutil
    entitlements
    flightcontrol
    gitutil
    grpcerrors
    progress
    resolver
    sshutil
    stack
    system
    testutil
    tracing
    wildcard
)

IF (OS_LINUX)
    RECURSE(
        appdefaults
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        appdefaults
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        appdefaults
    )
ENDIF()
