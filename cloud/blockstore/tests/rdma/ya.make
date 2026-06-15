RECURSE(
    image
)

# libibverbs has some adress/memory sanitizer failures
# which will cause failure in execution of rdma-test utility
# don't run test with sanitizers for now
IF (NOT SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        rdma-test
    )
ENDIF()
