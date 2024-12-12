IF (BUILD_CSI_DRIVER)

RECURSE(
    lib
    csi_sanity_tests
    e2e_tests_nfs
    e2e_tests_part1
    e2e_tests_part2
)

ENDIF()
