#include "device_client.h"

#include "public.h"

#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDeviceClient::TDeviceClient(
        TDuration releaseInactiveSessionsTimeout,
        TVector<TString> uuids,
        TLog log)
    : ReleaseInactiveSessionsTimeout(releaseInactiveSessionsTimeout)
    , Devices(MakeDevices(std::move(uuids)))
    , Log(std::move(log))
{}

TVector<NProto::TDiskAgentDeviceSession> TDeviceClient::GetSessions() const
{
    THashMap<TString, NProto::TDiskAgentDeviceSession> sessions;

    for (const auto& [id, state]: Devices) {
        TReadGuard g(state->Lock);

        if (!state->WriterSession.Id.empty()) {
            auto& session = sessions[state->WriterSession.Id];

            if (session.GetClientId().empty()) {
                const auto& ws = state->WriterSession;

                session.SetClientId(ws.Id);
                session.SetReadOnly(false);
                session.SetDiskId(state->DiskId);
                session.SetVolumeGeneration(state->VolumeGeneration);
                session.SetMountSeqNumber(ws.MountSeqNumber);
                session.SetLastActivityTs(ws.LastActivityTs.MicroSeconds());
            }
            *session.AddDeviceIds() = id;
        }

        for (const auto& rs: state->ReaderSessions) {
            auto& session = sessions[rs.Id];
            if (session.GetClientId().empty()) {
                session.SetClientId(rs.Id);
                session.SetReadOnly(true);
                session.SetDiskId(state->DiskId);
                session.SetVolumeGeneration(state->VolumeGeneration);
                session.SetMountSeqNumber(rs.MountSeqNumber);
                session.SetLastActivityTs(rs.LastActivityTs.MicroSeconds());
            }
            *session.AddDeviceIds() = id;
        }
    }

    TVector<NProto::TDiskAgentDeviceSession> r;
    r.reserve(sessions.size());

    for (auto&& [_, session]: sessions) {
        Sort(*session.MutableDeviceIds());

        r.push_back(std::move(session));
    }

    SortBy(r, [] (const auto& session) {
        return TStringBuf {session.GetClientId()};
    });

    return r;
}

TResultOrError<bool> TDeviceClient::AcquireDevices(
    const TVector<TString>& uuids,
    const TString& clientId,
    TInstant now,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber,
    const TString& diskId,
    ui32 volumeGeneration) const
{
    if (!clientId) {
        return MakeError(E_ARGUMENT, "empty client id");
    }

    // check devices
    for (const auto& uuid: uuids) {
        TDeviceState* deviceState = GetDeviceState(uuid);
        if (!deviceState) {
            return MakeError(E_NOT_FOUND, TStringBuilder()
                << "Device " << uuid.Quote() << " not found");
        }

        TReadGuard g(deviceState->Lock);

        if (deviceState->DiskId == diskId
                && deviceState->VolumeGeneration > volumeGeneration
                // backwards compat
                && volumeGeneration)
        {
            return MakeError(E_BS_INVALID_SESSION, TStringBuilder()
                << "AcquireDevices: "
                << "Outdated volume generation, DiskId=" << diskId.Quote()
                << ", VolumeGeneration: " << volumeGeneration
                << ", LastGeneration: " << deviceState->VolumeGeneration);
        }

        if (IsReadWriteMode(accessMode)
                && deviceState->WriterSession.Id
                && deviceState->WriterSession.Id != clientId
                && deviceState->WriterSession.MountSeqNumber >= mountSeqNumber
                && deviceState->WriterSession.LastActivityTs
                    + ReleaseInactiveSessionsTimeout
                    > now)
        {
            return MakeError(E_BS_INVALID_SESSION, TStringBuilder()
                << "Error acquiring device " << uuid.Quote()
                << " with client " << clientId.Quote()
                << " already acquired by another client: "
                << deviceState->WriterSession.Id.Quote());
        }
    }

    bool somethingHasChanged = false;

    // acquire devices
    for (const auto& uuid: uuids) {
        TDeviceState& ds = *GetDeviceState(uuid);

        TWriteGuard g(ds.Lock);

        auto s = FindIf(
            ds.ReaderSessions.begin(),
            ds.ReaderSessions.end(),
            [&] (const TSessionInfo& s) {
                return s.Id == clientId;
            }
        );

        if (ds.DiskId != diskId || ds.VolumeGeneration != volumeGeneration) {
            somethingHasChanged = true;
        }

        ds.DiskId = diskId;
        ds.VolumeGeneration = volumeGeneration;

        if (!IsReadWriteMode(accessMode)) {
            if (clientId == ds.WriterSession.Id) {
                ds.WriterSession = {};
                STORAGE_INFO("Device %s was released by client %s for writing.",
                    uuid.Quote().c_str(), clientId.c_str());

                somethingHasChanged = true;
            }

            // a new session or activation of a stale session
            if (s == ds.ReaderSessions.end() ||
                s->LastActivityTs + ReleaseInactiveSessionsTimeout <= now)
            {
                somethingHasChanged = true;
            }

            if (s == ds.ReaderSessions.end()) {
                ds.ReaderSessions.push_back({clientId, now});
                STORAGE_INFO("Device %s was acquired by client %s for reading.",
                    uuid.Quote().c_str(), clientId.c_str());
            } else if (now > s->LastActivityTs) {
                s->LastActivityTs = now;
            }
        } else {
            if (s != ds.ReaderSessions.end()) {
                ds.ReaderSessions.erase(s);
                STORAGE_INFO("Device %s was released by client %s for reading.",
                    uuid.Quote().c_str(), clientId.c_str());

                somethingHasChanged = true;
            }

            if (ds.WriterSession.Id != clientId) {
                STORAGE_INFO("Device %s was acquired by client %s for writing.",
                    uuid.Quote().c_str(), clientId.c_str());

                somethingHasChanged = true;
            }

            if (ds.WriterSession.MountSeqNumber != mountSeqNumber ||
                ds.WriterSession.LastActivityTs +
                        ReleaseInactiveSessionsTimeout <=
                    now)
            {
                somethingHasChanged = true;
            }

            ds.WriterSession.Id = clientId;
            ds.WriterSession.LastActivityTs =
                Max(ds.WriterSession.LastActivityTs, now);
            ds.WriterSession.MountSeqNumber = mountSeqNumber;
        }
    }

    return {somethingHasChanged};
}

NCloud::NProto::TError TDeviceClient::ReleaseDevices(
    const TVector<TString>& uuids,
    const TString& clientId,
    const TString& diskId,
    ui32 volumeGeneration) const
{
    if (!clientId) {
        return MakeError(E_ARGUMENT, "empty client id");
    }

    for (const auto& uuid: uuids) {
        auto* deviceState = GetDeviceState(uuid);

        if (deviceState == nullptr) {
            continue; // TODO: log
        }

        TWriteGuard g(deviceState->Lock);

        if (deviceState->DiskId == diskId
                && deviceState->VolumeGeneration > volumeGeneration
                // backwards compat
                && volumeGeneration)
        {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "ReleaseDevices: "
                << "outdated volume generation, DiskId=" << diskId.Quote()
                << ", VolumeGeneration: " << volumeGeneration
                << ", LastGeneration: " << deviceState->VolumeGeneration);
        }

        deviceState->DiskId = diskId;
        deviceState->VolumeGeneration = volumeGeneration;

        auto s = FindIf(
            deviceState->ReaderSessions.begin(),
            deviceState->ReaderSessions.end(),
            [&] (const TSessionInfo& s) {
                return s.Id == clientId;
            }
        );

        if (s != deviceState->ReaderSessions.end()) {
            deviceState->ReaderSessions.erase(s);
            STORAGE_INFO("Device %s was released by client %s for reading.",
                    uuid.Quote().c_str(), clientId.c_str());
        } else if (deviceState->WriterSession.Id == clientId
            || clientId == AnyWriterClientId)
        {
            STORAGE_INFO("Device %s was released by client %s for writing.",
                    uuid.Quote().c_str(), clientId.c_str());
            deviceState->WriterSession = {};
        }
    }

    return {};
}

NCloud::NProto::TError TDeviceClient::AccessDevice(
    const TString& uuid,
    const TString& clientId,
    NProto::EVolumeAccessMode accessMode) const
{
    if (!clientId) {
        return MakeError(E_ARGUMENT, "empty client id");
    }

    auto* deviceState = GetDeviceState(uuid);
    if (!deviceState) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "Device " << uuid.Quote() << " not found");
    }

    TReadGuard g(deviceState->Lock);

    bool acquired = false;
    if (clientId == BackgroundOpsClientId || clientId == CopyVolumeClientId) {
        // it's fine to accept migration writes if this device is not acquired
        // migration might be in progress even for an unmounted volume
        acquired = !IsReadWriteMode(accessMode)
            || deviceState->WriterSession.Id.empty();
    } else if (clientId == CheckHealthClientId || clientId == CheckRangeClientId) {
        acquired = accessMode == NProto::VOLUME_ACCESS_READ_ONLY;
    } else {
        acquired = clientId == deviceState->WriterSession.Id;

        if (!acquired && !IsReadWriteMode(accessMode)) {
            auto s = FindIf(
                deviceState->ReaderSessions.begin(),
                deviceState->ReaderSessions.end(),
                [&] (const TSessionInfo& s) {
                    return s.Id == clientId;
                }
            );

            acquired = s != deviceState->ReaderSessions.end();
        }
    }

    if (!acquired) {
        TStringBuilder allReaders;
        for (const auto& reader: deviceState->ReaderSessions) {
            bool isFirst = allReaders.empty();
            allReaders << reader.Id.Quote();
            if (!isFirst) {
                allReaders << ", ";
            }
        }
        return MakeError(
            E_BS_INVALID_SESSION,
            TStringBuilder()
                << "Device " << uuid.Quote() << " not acquired by client "
                << clientId.Quote() << ", current active writer: "
                << deviceState->WriterSession.Id.Quote()
                << ", current readers: [" << allReaders << "]");
    }

    return {};
}

TDeviceClient::TSessionInfo TDeviceClient::GetWriterSession(
    const TString& uuid) const
{
    if (auto* deviceState = GetDeviceState(uuid)) {
        TReadGuard g(deviceState->Lock);
        return deviceState->WriterSession;
    }

    return {};
}

TVector<TDeviceClient::TSessionInfo> TDeviceClient::GetReaderSessions(
    const TString& uuid) const
{
    if (auto* deviceState = GetDeviceState(uuid)) {
        TReadGuard g(deviceState->Lock);
        return deviceState->ReaderSessions;
    }

    return {};
}

void TDeviceClient::DisableDevice(const TString& uuid) const
{
    SetDeviceIOErrorCode(uuid, E_IO);
}

void TDeviceClient::SuspendDevice(const TString& uuid) const
{
    SetDeviceIOErrorCode(uuid, E_REJECTED);
}

void TDeviceClient::SetDeviceIOErrorCode(
    const TString& uuid,
    std::optional<ui32> errorCode) const
{
    if (auto* deviceState = GetDeviceState(uuid)) {
        TWriteGuard g(deviceState->Lock);
        deviceState->IOErrorCode = errorCode;
    }
}

void TDeviceClient::EnableDevice(const TString& uuid) const
{
    SetDeviceIOErrorCode(uuid, std::nullopt);
}

std::optional<ui32> TDeviceClient::GetDeviceIOErrorCode(
    const TString& uuid) const
{
    if (auto* deviceState = GetDeviceState(uuid)) {
        TReadGuard g(deviceState->Lock);
        return deviceState->IOErrorCode;
    }

    return {};
}

bool TDeviceClient::IsDeviceDisabled(const TString& uuid) const
{
    return GetDeviceIOErrorCode(uuid).value_or(S_OK) == E_IO;
}

bool TDeviceClient::IsDeviceSuspended(const TString& uuid) const
{
    return GetDeviceIOErrorCode(uuid).value_or(S_OK) == E_REJECTED;
}

bool TDeviceClient::IsDeviceEnabled(const TString& uuid) const
{
    return !GetDeviceIOErrorCode(uuid).has_value();
}

// static
TDeviceClient::TDevicesState TDeviceClient::MakeDevices(TVector<TString> uuids)
{
    TDevicesState result;
    for (auto& uuid: uuids) {
        result.emplace(std::move(uuid), std::make_unique<TDeviceState>());
    }
    return result;
}

TDeviceClient::TDeviceState* TDeviceClient::GetDeviceState(
    const TString& uuid) const
{
    auto it = Devices.find(uuid);
    if (it == Devices.end()) {
        return nullptr;
    }
    auto* result = it->second.get();
    Y_ABORT_UNLESS(result);
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
