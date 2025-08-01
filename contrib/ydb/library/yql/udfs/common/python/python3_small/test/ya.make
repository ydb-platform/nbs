YQL_UDF_TEST_CONTRIB()

TIMEOUT(300)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/library/yql/udfs/common/python/python3_small
)

END()
