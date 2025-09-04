LIBRARY()

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    cell_maker.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/core/scheme_types
    contrib/ydb/library/binary_json
    contrib/ydb/library/dynumber
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/public/decimal
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/libs/double-conversion
    library/cpp/json
    library/cpp/json/yson
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
)

YQL_LAST_ABI_VERSION()

END()
