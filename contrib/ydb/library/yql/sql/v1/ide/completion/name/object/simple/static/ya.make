LIBRARY()

SRCS(
    schema_json.cpp
    schema.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple
    library/cpp/json
)

END()
