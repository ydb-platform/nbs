#pragma once

#include "nvme.h"

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

using NvmeDeallocateHistory = TVector<std::tuple<ui64, ui64>>;
using NvmeDeallocateHistoryPtr = std::shared_ptr<NvmeDeallocateHistory>;

INvmeManagerPtr CreateNvmeManagerStub(
    bool isDeviceSsd = true,
    NvmeDeallocateHistoryPtr deallocateHistory = nullptr);

}   // namespace NCloud::NBlockStore::NNvme
