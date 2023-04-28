#pragma once

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/storage/core/protos/error.pb.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/rwlock.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDeviceClient final
{
public:
    struct TSessionInfo
    {
        TString Id;
        TInstant LastActivityTs;
        ui64 MountSeqNumber = 0;
    };

private:
    struct TDeviceState
    {
        TString DiskId;
        ui32 VolumeGeneration = 0;
        TSessionInfo WriterSession;
        TVector<TSessionInfo> ReaderSessions;
        TRWMutex Lock;
    };

    const TDuration ReleaseInactiveSessionsTimeout;

    THashMap<TString, TDeviceState> Devices;

public:
    TDeviceClient(
        TDuration releaseInactiveSessionsTimeout,
        TVector<TString> uuids);

    TDeviceClient(const TDeviceClient&) = delete;
    TDeviceClient& operator=(const TDeviceClient&) = delete;

    TDeviceClient(TDeviceClient&&) noexcept = default;
    TDeviceClient& operator=(TDeviceClient&&) noexcept = default;

    NCloud::NProto::TError AcquireDevices(
        const TVector<TString>& uuids,
        const TString& sessionId,
        TInstant now,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        const TString& diskId,
        ui32 volumeGeneration);

    NCloud::NProto::TError ReleaseDevices(
        const TVector<TString>& uuids,
        const TString& sessionId,
        const TString& diskId,
        ui32 volumeGeneration);

    NCloud::NProto::TError AccessDevice(
        const TString& uuid,
        const TString& sessionId,
        NProto::EVolumeAccessMode accessMode) const;

    TSessionInfo GetWriterSession(const TString& uuid) const;
    TVector<TSessionInfo> GetReaderSessions(const TString& uuid) const;
};

}   // namespace NCloud::NBlockStore::NStorage
