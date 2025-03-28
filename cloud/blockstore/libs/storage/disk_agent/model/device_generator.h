#pragma once

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/logger/log.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDeviceGenerator
{
private:
    TLog Log;
    TString AgentId;

    TVector<NProto::TFileDeviceArgs> Result;

public:
    TDeviceGenerator(TLog log, TString agentId);

    NProto::TError operator () (
        const TString& path,
        const NProto::TStorageDiscoveryConfig::TPoolConfig& poolConfig,
        ui32 deviceNumber,
        ui32 maxDeviceCount,
        ui32 blockSize,
        ui64 fileSize);

    TVector<NProto::TFileDeviceArgs> ExtractResult();

    void AddPossibleUUIDSForDevice(
        const NProto::TStorageDiscoveryConfig::TPoolConfig& poolConfig,
        ui32 deviceNumber,
        THashSet<TString>& setToAddUUIDs);

private:
    TString CreateDeviceId(ui32 deviceNumber, const TString& suffix) const;
    TString CreateDeviceId(
        ui32 deviceNumber,
        const TString& suffix,
        ui32 deviceIndex) const;
};

}   // namespace NCloud::NBlockStore::NStorage
