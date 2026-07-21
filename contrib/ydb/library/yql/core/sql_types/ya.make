LIBRARY()

SRCS(
    block.h
    match_recognize.h
    match_recognize.cpp
    normalize_name.cpp
    simple_types.h
    simple_types.cpp
    yql_atom_enums.h
    yql_callable_names.h
)

PEERDIR(
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/public/decimal
)

GENERATE_ENUM_SERIALIZATION(hopping.h)
GENERATE_ENUM_SERIALIZATION(match_recognize.h)
GENERATE_ENUM_SERIALIZATION(yql_atom_enums.h)

END()

RECURSE_FOR_TESTS(
    ut
)
