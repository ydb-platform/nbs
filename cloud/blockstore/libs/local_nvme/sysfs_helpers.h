#pragma once

#include "public.h"

#include "private.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/folder/fwd.h>
#include <util/generic/fwd.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ISysFs
{
    virtual ~ISysFs() = default;

    virtual auto GetDriverForPCIDevice(const TString& pciAddr) -> TString = 0;

    virtual void BindPCIDeviceToDriver(
        const TString& pciAddr,
        const TString& driverName) = 0;

    virtual auto GetNVMeCtrlNameFromPCIAddr(const TString& pciAddr)
        -> TString = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISysFsPtr CreateSysFs(TFsPath sysFsRoot);

}   // namespace NCloud::NBlockStore
