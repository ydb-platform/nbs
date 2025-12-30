# Executables should not depend on test definitions and YDB definitions, which
# are redefined in NBS/NFS. To get an error at the build stage, and undefined
# behavior due to ODR violations, such dependencies should be placed here.

CHECK_DEPENDENT_DIRS(
    DENY contrib/ydb/apps/version
    DENY contrib/ydb/core/testlib/basics
)
