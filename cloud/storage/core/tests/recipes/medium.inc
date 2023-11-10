SIZE(MEDIUM)
TIMEOUT(600)

TAG(
    sb:ttl=3
    sb:logs_ttl=3
    sb:ssd
    sb:MULTISLOT
    sb:cores=8
)

IF (NOT OPENSOURCE)
    FORK_SUBTESTS()
    SPLIT_FACTOR(10)
ENDIF()

REQUIREMENTS(
    cpu:4
    ram:16
)
