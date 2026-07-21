YQL_UDF_TEST()

DEPENDS(contrib/ydb/library/yql/udfs/common/url_base)

TIMEOUT(300)

SIZE(MEDIUM)

DATA(
    sbr://451427803 # Robots.in
)

END()
