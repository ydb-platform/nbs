#pragma once

#include "private.h"

#include <cloud/blockstore/libs/rdma/iface/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IRunnablePtr CreateTestTarget(
    TOptionsPtr options,
    ITaskQueuePtr taskQueue,
    IStoragePtr storage,
    NRdma::IServerPtr server);

}   // namespace NCloud::NBlockStore
