#include "tablet.h"

#include <contrib/ydb/core/engine/minikql/flat_local_tx_factory.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NKikimr::NTabletFlatExecutor::IMiniKQLFactory* NewMiniKQLFactory()
{
    return new NKikimr::NMiniKQL::TMiniKQLFactory();
}

ui64 CreateTransactionId()
{
    static std::atomic<ui64> TransactionGenerator{1};
    return ++TransactionGenerator;
}

}   // namespace NCloud::NBlockStore::NStorage
