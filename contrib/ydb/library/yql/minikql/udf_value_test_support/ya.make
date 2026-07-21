LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/utils/random_data_generator
)

END()

RECURSE_FOR_TESTS(
    ut
)
