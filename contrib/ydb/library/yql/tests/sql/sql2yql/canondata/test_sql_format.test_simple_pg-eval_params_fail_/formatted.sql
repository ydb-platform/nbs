/* custom error: Error: Failed to parse data for parameter: $x, reason: YSON parsing failed: contrib/ydb/library/yql/minikql/mkql_terminator.cpp:xxx: ERROR:  malformed array literal: "NOT VALID ARRAY" */
DECLARE $x AS _pgbytea;

SELECT
    $x
;
