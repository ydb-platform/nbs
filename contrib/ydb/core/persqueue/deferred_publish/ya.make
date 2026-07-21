LIBRARY()

SRCS(
    delete_publication_query.cpp
    describe_publication_query.cpp
    destination_blob.cpp
    finalize_publication_actor.cpp
    get_destination_blob_query.cpp
    insert_publication_query.cpp
    list_destinations_query.cpp
    list_publications_query.cpp
    query_utils.cpp
    registry_actor.cpp
    tables_creator.cpp
    upsert_destination_query.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/core/protos
    contrib/ydb/library/aclib
    contrib/ydb/library/actors/core
    contrib/ydb/library/query_actor
    contrib/ydb/library/services
    contrib/ydb/library/persqueue/topic_parser
    contrib/ydb/library/table_creator
    contrib/ydb/public/lib/scheme_types
    contrib/ydb/library/yql/public/issue
)

END()
