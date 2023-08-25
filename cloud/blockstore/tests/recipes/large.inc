SIZE(LARGE)
TIMEOUT(3600)

TAG(
    ya:fat
    ya:force_sandbox
    sb:ttl=3
    sb:logs_ttl=3
    sb:ssd
    sb:cores=16
)

FORK_SUBTESTS()
SPLIT_FACTOR(5)

REQUIREMENTS(
    cpu:8
    ram:32
)
