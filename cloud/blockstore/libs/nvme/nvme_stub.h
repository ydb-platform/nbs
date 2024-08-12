#pragma once

#include "nvme.h"

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

struct TDeallocateReq
{
    ui64 Offset;
    ui64 Size;

    bool operator==(const TDeallocateReq& other) const
    {
        return Offset == other.Offset && Size == other.Size;
    }
};

using TNvmeDeallocateHistory = TVector<TDeallocateReq>;
using TNvmeDeallocateHistoryPtr = std::shared_ptr<TNvmeDeallocateHistory>;

INvmeManagerPtr CreateNvmeManagerStub(
    bool isDeviceSsd = true,
    TNvmeDeallocateHistoryPtr deallocateHistory = nullptr);

}   // namespace NCloud::NBlockStore::NNvme
