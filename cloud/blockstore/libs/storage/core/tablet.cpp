#include "tablet.h"

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NKikimr::NTabletFlatExecutor::IMiniKQLFactory* NewMiniKQLFactory()
{
    return new NKikimr::NMiniKQL::TMiniKQLFactory();
}

}   // namespace NCloud::NBlockStore::NStorage
