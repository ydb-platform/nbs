ADDINCL(
    contrib/libs/lz4
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/bootstrap
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/parser
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/replication
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/replication/logical
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/adt
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/misc
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/sort
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/common
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/port
)

IF (OS_WINDOWS)
    ADDINCL(
        contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include
        contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include/port/win32
        contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include/port/win32_msvc
    )
ENDIF()
