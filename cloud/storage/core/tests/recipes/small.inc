SIZE(SMALL)
TIMEOUT(60)

TAG(
    sb:ttl=3
    sb:logs_ttl=3
    sb:ssd
    sb:MULTISLOT
    sb:cores=4
)

IF (NOT OPENSOURCE)
    FORK_SUBTESTS()
    SPLIT_FACTOR(15)
ENDIF()

REQUIREMENTS(
    cpu:2
    ram:8
)
