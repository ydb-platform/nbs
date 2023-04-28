#include "device_client.h"

#include "public.h"

#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDeviceClient::TDeviceClient(
        TDuration releaseInactiveSessionsTimeout,
        TVector<TString> uuids)
    : ReleaseInactiveSessionsTimeout(releaseInactiveSessionsTimeout)
{
    for (auto& uuid: uuids) {
        Devices[std::move(uuid)];
    }
}

NCloud::NProto::TError TDeviceClient::AcquireDevices(
    const TVector<TString>& uuids,
    const TString& sessionId,
    TInstant now,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber,
    const TString& diskId,
    ui32 volumeGeneration)
{
    if (!sessionId) {
        return MakeError(E_ARGUMENT, "empty session id");
    }

    for (const auto& uuid: uuids) {
        auto it = Devices.find(uuid);

        if (it == Devices.end()) {
            return MakeError(E_NOT_FOUND, TStringBuilder()
                << "Device " << uuid.Quote() << " not found");
        }

        TReadGuard g(it->second.Lock);

        if (it->second.DiskId == diskId
                && it->second.VolumeGeneration > volumeGeneration
                // backwards compat
                && volumeGeneration)
        {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "AcquireDevices: "
                << "Outdated volume generation, DiskId=" << diskId.Quote()
                << ", VolumeGeneration: " << volumeGeneration
                << ", LastGeneration: " << it->second.VolumeGeneration);
        }

        it->second.DiskId = diskId;
        it->second.VolumeGeneration = volumeGeneration;

        if (IsReadWriteMode(accessMode)
                && it->second.WriterSession.Id
                && it->second.WriterSession.Id != sessionId
                && it->second.WriterSession.MountSeqNumber >= mountSeqNumber
                && it->second.WriterSession.LastActivityTs
                    + ReleaseInactiveSessionsTimeout
                    > now)
        {
            return MakeError(E_BS_INVALID_SESSION, TStringBuilder()
                << "Device " << uuid.Quote()
                << " already acquired by another session: "
                << it->second.WriterSession.Id);
        }
    }

    for (const auto& uuid: uuids) {
        auto& ds = Devices[uuid];

        TWriteGuard g(ds.Lock);

        auto s = FindIf(
            ds.ReaderSessions.begin(),
            ds.ReaderSessions.end(),
            [&] (const TSessionInfo& s) {
                return s.Id == sessionId;
            }
        );

        ds.DiskId = diskId;
        ds.VolumeGeneration = volumeGeneration;

        if (!IsReadWriteMode(accessMode)) {
            if (s == ds.ReaderSessions.end()) {
                ds.ReaderSessions.push_back({sessionId, now});
            } else if (now > s->LastActivityTs) {
                s->LastActivityTs = now;
            }

            if (sessionId == ds.WriterSession.Id) {
                ds.WriterSession = {};
            }
        } else {
            ds.WriterSession.Id = sessionId;
            ds.WriterSession.LastActivityTs =
                Max(ds.WriterSession.LastActivityTs, now);
            ds.WriterSession.MountSeqNumber = mountSeqNumber;

            if (s != ds.ReaderSessions.end()) {
                ds.ReaderSessions.erase(s);
            }
        }
    }

    return {};
}

NCloud::NProto::TError TDeviceClient::ReleaseDevices(
    const TVector<TString>& uuids,
    const TString& sessionId,
    const TString& diskId,
    ui32 volumeGeneration)
{
    if (!sessionId) {
        return MakeError(E_ARGUMENT, "empty session id");
    }

    for (const auto& uuid : uuids) {
        auto it = Devices.find(uuid);

        if (it == Devices.end()) {
            continue; // TODO: log
        }

        TWriteGuard g(it->second.Lock);

        if (it->second.DiskId == diskId
                && it->second.VolumeGeneration > volumeGeneration
                // backwards compat
                && volumeGeneration)
        {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "ReleaseDevices: "
                << "outdated volume generation, DiskId=" << diskId.Quote()
                << ", VolumeGeneration: " << volumeGeneration
                << ", LastGeneration: " << it->second.VolumeGeneration);
        }

        it->second.DiskId = diskId;
        it->second.VolumeGeneration = volumeGeneration;

        auto s = FindIf(
            it->second.ReaderSessions.begin(),
            it->second.ReaderSessions.end(),
            [&] (const TSessionInfo& s) {
                return s.Id == sessionId;
            }
        );

        if (s != it->second.ReaderSessions.end()) {
            it->second.ReaderSessions.erase(s);
        } else if (it->second.WriterSession.Id == sessionId
            || sessionId == AnyWriterSessionId)
        {
            it->second.WriterSession = {};
        }
    }

    return {};
}

NCloud::NProto::TError TDeviceClient::AccessDevice(
    const TString& uuid,
    const TString& sessionId,
    NProto::EVolumeAccessMode accessMode) const
{
    if (!sessionId) {
        return MakeError(E_ARGUMENT, "empty session id");
    }

    auto it = Devices.find(uuid);
    if (it == Devices.cend()) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "Device " << uuid.Quote() << " not found");
    }

    TReadGuard g(it->second.Lock);

    bool acquired = false;
    if (sessionId == BackgroundOpsSessionId) {
        // it's fine to accept migration writes if this device is not acquired
        // migration might be in progress even for an unmounted volume
        acquired = !IsReadWriteMode(accessMode)
            || it->second.WriterSession.Id.empty();
    } else {
        acquired = sessionId == it->second.WriterSession.Id;

        if (!acquired && !IsReadWriteMode(accessMode)) {
            auto s = FindIf(
                it->second.ReaderSessions.begin(),
                it->second.ReaderSessions.end(),
                [&] (const TSessionInfo& s) {
                    return s.Id == sessionId;
                }
            );

            acquired = s != it->second.ReaderSessions.end();
        }
    }

    if (!acquired) {
        return MakeError(E_BS_INVALID_SESSION, TStringBuilder()
            << "Device " << uuid.Quote() << " not acquired by session " << sessionId
            << ", current active writer: " << it->second.WriterSession.Id);
    }

    return {};
}

TDeviceClient::TSessionInfo TDeviceClient::GetWriterSession(
    const TString& uuid) const
{
    auto it = Devices.find(uuid);
    if (it != Devices.cend()) {
        TReadGuard g(it->second.Lock);
        return it->second.WriterSession;
    }

    return {};
}

TVector<TDeviceClient::TSessionInfo> TDeviceClient::GetReaderSessions(
    const TString& uuid) const
{
    auto it = Devices.find(uuid);
    if (it != Devices.cend()) {
        TReadGuard g(it->second.Lock);
        return it->second.ReaderSessions;
    }

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
