IF (BUILD_CSI_DRIVER)

RECURSE(
    lib
    csi_sanity_tests
    e2e_tests
)

ENDIF()
