SIZE(MEDIUM)
TIMEOUT(600)

TAG(
    sb:ttl=3
    sb:logs_ttl=3
    sb:ssd
    sb:MULTISLOT
    sb:cores=8
)

FORK_SUBTESTS()
SPLIT_FACTOR(7)

REQUIREMENTS(
    cpu:4
    ram:16
)

DATA(arcadia/cloud/filestore/tests/common_configs)
