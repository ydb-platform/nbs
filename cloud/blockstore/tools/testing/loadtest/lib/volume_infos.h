#pragma once

#include "public.h"

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceAddress
{
    TString AgentId;
    TString Name;
    TString DeviceUUID;
};

struct TVolumeInfo
{
    TVector<TString> AgentIds;
    TVector<TDeviceAddress> Devices;
};

class TVolumeInfos
{
private:
    THashMap<TString, TVolumeInfo> DiskId2Info;
    mutable TAdaptiveLock Lock;

public:
    void UpdateAgentIds(TStringBuf diskId, TVector<TString> agentIds);
    void UpdateDevices(TStringBuf diskId, TVector<TDeviceAddress> devices);
    bool GetVolumeInfo(TStringBuf diskId, TVolumeInfo* volumeInfo) const;
};

}   // namespace NCloud::NBlockStore::NLoadTest
