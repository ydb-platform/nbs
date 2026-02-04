#pragma once

#include <cloud/blockstore/libs/local_nvme/public.h>
#include <cloud/blockstore/libs/storage/protos/local_nvme.pb.h>

#include <util/generic/fwd.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeServiceMock(
    TVector<NProto::TNVMeDevice> disks);

}   // namespace NCloud::NBlockStore
