#pragma once

#include "query.h"

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/tablet_flat/flat_dbase_scheme.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct TBoundColumn
{
    ui32 Id;
    NKikimr::NScheme::TTypeInfo Type;
    // Position in the row fetched using ReadColumnIds.
    size_t ReadPosition;
};

struct TBoundSelect
{
    TSelect Query;

    ui32 TableId;

    // Unique columns needed by the projection and predicates.
    TVector<ui32> ReadColumnIds;

    // Preserves projection order and duplicates; SELECT * uses column ID order.
    TVector<size_t> OutputPositions;

    THashMap<TString, TBoundColumn> Columns;
};

// Consumes the move-only AST. Names must match the runtime schema exactly.
TResultOrError<TBoundSelect> Bind(
    TSelect query, const NKikimr::NTable::TScheme& schema);

}   // namespace NCloud::NFileStore::NStorage::NQuery
