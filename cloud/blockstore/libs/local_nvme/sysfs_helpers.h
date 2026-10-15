#pragma once
#include "public.h"

#include "private.h"

#include <cloud/blockstore/libs/local_nvme/protos/local_nvme.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/folder/fwd.h>
#include <util/generic/fwd.h>
#include <util/system/file.h>

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

    virtual auto GetNVMeDeviceFromPCIAddr(const TString& pciAddr)
        -> NProto::TNVMeDevice = 0;

    virtual auto GetVfioDeviceForPCIDevice(const TString& pciAddr)
        -> TString = 0;

    // Throws E_PRECONDITION_FAILED if the VFIO group is busy; other open errors
    // retain their system codes. Keep the returned fd open until the device is
    // unbound to block other VFIO group users and cdev/iommufd bindings.
    // Unbound cdev fds are not covered by this guard.
    virtual auto OpenVfioGroupForPCIDevice(const TString& pciAddr)
        -> TFileHandle = 0;

    [[nodiscard]] virtual auto IsVfioDevSupported() const -> bool = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISysFsPtr CreateSysFs(TFsPath sysFsRoot, TFsPath devFsRoot);

}   // namespace NCloud::NBlockStore
