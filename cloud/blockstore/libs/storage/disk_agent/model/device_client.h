#pragma once

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/rwlock.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. Public methods can be called from any thread.
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

        // If the value is set, it is returned as the result of IO
        // operations and the device is considered to be disabled.
        std::optional<ui32> IOErrorCode;

        TRWMutex Lock;
    };
    using TDevicesState = THashMap<TString, std::unique_ptr<TDeviceState>>;

    const TDuration ReleaseInactiveSessionsTimeout;
    const TDevicesState Devices;
    TLog Log;

public:
    TDeviceClient(
        TDuration releaseInactiveSessionsTimeout,
        TVector<TString> uuids,
        TLog log);

    TDeviceClient(const TDeviceClient&) = delete;
    TDeviceClient& operator=(const TDeviceClient&) = delete;

    TDeviceClient(TDeviceClient&&) noexcept = delete;
    TDeviceClient& operator=(TDeviceClient&&) noexcept = delete;

    // returns `true` if any session has been updated
    // (excluding `LastActivityTs` field) or a new one has been added.
    TResultOrError<bool> AcquireDevices(
        const TVector<TString>& uuids,
        const TString& clientId,
        TInstant now,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        const TString& diskId,
        ui32 volumeGeneration) const;

    NCloud::NProto::TError ReleaseDevices(
        const TVector<TString>& uuids,
        const TString& clientId,
        const TString& diskId,
        ui32 volumeGeneration) const;

    NCloud::NProto::TError AccessDevice(
        const TString& uuid,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) const;

    TSessionInfo GetWriterSession(const TString& uuid) const;
    TVector<TSessionInfo> GetReaderSessions(const TString& uuid) const;

    // Return E_IO error on I/O operations.
    void DisableDevice(const TString& uuid) const;

    // Same as DisableDevice but return E_REJECTED.
    void SuspendDevice(const TString& uuid) const;

    void EnableDevice(const TString& uuid) const;

    bool IsDeviceDisabled(const TString& uuid) const;
    bool IsDeviceSuspended(const TString& uuid) const;
    bool IsDeviceEnabled(const TString& uuid) const;
    std::optional<ui32> GetDeviceIOErrorCode(const TString& uuid) const;

    TVector<NProto::TDiskAgentDeviceSession> GetSessions() const;

private:
    static TDevicesState MakeDevices(TVector<TString> uuids);
    [[nodiscard]] TDeviceState* GetDeviceState(const TString& uuid) const;

    void SetDeviceIOErrorCode(
        const TString& uuid,
        std::optional<ui32> errorCode) const;
};

}   // namespace NCloud::NBlockStore::NStorage
