#include "volume_infos.h"

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

void TVolumeInfos::UpdateAgentIds(TStringBuf diskId, TVector<TString> agentIds)
{
    with_lock (Lock) {
        DiskId2Info[diskId].AgentIds = std::move(agentIds);
    }
}

void TVolumeInfos::UpdateDevices(
    TStringBuf diskId,
    TVector<TDeviceAddress> devices)
{
    with_lock (Lock) {
        DiskId2Info[diskId].Devices = std::move(devices);
    }
}

bool TVolumeInfos::GetVolumeInfo(
    TStringBuf diskId,
    TVolumeInfo* volumeInfo) const
{
    with_lock (Lock) {
        if (auto* ptr = DiskId2Info.FindPtr(diskId)) {
            *volumeInfo = *ptr;
            return true;
        }
    }

    return false;
}

}   // namespace NCloud::NBlockStore::NLoadTest
