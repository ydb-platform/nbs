#pragma once

#include "public.h"

#include <cloud/blockstore/config/local_nvme.pb.h>

#include <util/datetime/base.h>
#include <util/generic/fwd.h>

#include <optional>

class IOutputStream;

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TNVMeLockdownConfig
{
private:
    const NProto::TLocalNVMeConfig::TLockdownConfig Proto;

public:
    explicit TNVMeLockdownConfig(
        const NProto::TLocalNVMeConfig::TLockdownConfig& proto);
    ~TNVMeLockdownConfig();

    [[nodiscard]] TVector<ui8> GetAllowedAdminOpcodes() const;
    [[nodiscard]] TVector<ui8> GetAllowedSetFeatureIds() const;
    [[nodiscard]] bool GetBlockLockdownCommand() const;
};

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeConfig
{
private:
    const NProto::TLocalNVMeConfig Proto;

public:
    explicit TLocalNVMeConfig(NProto::TLocalNVMeConfig proto);
    ~TLocalNVMeConfig();

    [[nodiscard]] const NProto::TLocalNVMeConfig& GetConfigProto() const;

    [[nodiscard]] TString GetDevicesSourceUri() const;
    [[nodiscard]] TString GetStateCacheFilePath() const;
    [[nodiscard]] TDuration GetUpdateDevicesInterval() const;
    [[nodiscard]] TDuration GetUpdateCountersInterval() const;
    [[nodiscard]] std::optional<TNVMeLockdownConfig> GetLockdownConfig() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore
