PACKAGE()

# go install github.com/kubernetes-csi/csi-test/v5/cmd/csi-sanity@latest 
# ls -la ~/go/bin/csi-sanity

IF(ARCH_X86_64)
    FROM_SANDBOX(
        FILE
        1233456600
        RENAME RESOURCE
        OUT_NOAUTO csi-sanity
        EXECUTABLE)
ELSEIF (ARCH_ARM64)
    FROM_SANDBOX(
        FILE
        1233456601
        RENAME RESOURCE
        OUT_NOAUTO csi-sanity
        EXECUTABLE)
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported platform")
ENDIF()

END()
