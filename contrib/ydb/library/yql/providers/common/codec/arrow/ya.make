LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/yql/minikql/arrow
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/public/udf/arrow
)

SRCS(
    yql_codec_buf_input_stream.cpp
    yql_codec_buf_output_stream.cpp
)

YQL_LAST_ABI_VERSION()

END()
