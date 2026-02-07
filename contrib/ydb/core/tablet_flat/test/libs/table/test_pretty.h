#pragma once

#include <contrib/ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <contrib/ydb/core/tablet_flat/flat_row_remap.h>
#include <contrib/ydb/core/tablet_flat/flat_mem_warm.h>
#include <contrib/ydb/core/scheme/scheme_type_id.h>

namespace NKikimr {
namespace NTable {

    TString PrintRow(const TDbTupleRef& row);
    TString PrintRow(const TMemIter&);

}
}
