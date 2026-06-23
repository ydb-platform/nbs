# The actor-level balancer tests pull in the full storage test runtime and YDB testlib.
# With UBSAN this currently produces a binary that exceeds x86_64 PC-relative
# relocation limits during linking. Keep the smaller state tests enabled there.
IF (SANITIZER_TYPE == "undefined")
    RECURSE_FOR_TESTS(
        state
    )
ELSE()
    RECURSE_FOR_TESTS(
        balancer
        state
    )
ENDIF()
