#pragma once

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class ESecureEraseStatus
{
    // Secure erase is scheduled and waiting to start.
    Wait,
    // Secure erase is currently running on the device.
    InProgress,
    // Secure erase has finished and its result is stored in the record.
    Completed,
};

enum class EIoApproveStatus
{
    // I/O may proceed because secure erase is absent or completed.
    Allow,
    // I/O must be rejected because secure erase is waiting or in progress.
    Decline,
};

struct TSecureErase
{
    TString DeviceName;
    ESecureEraseStatus Status = ESecureEraseStatus::Wait;
    ui32 Generation = 0;
    ui64 IdempotencyKey = 0;
    TDeque<TRequestInfoPtr> Requests;
    NProto::TError Error;
};

////////////////////////////////////////////////////////////////////////////////

class TSecureEraseState
{
private:
    THashMap<TString, TSecureErase> SecureErases;
    THashSet<TString> DevicesInProgress;
    THashSet<TString> DevicesNamesInProgress;
    ui32 CurrentGeneration = 0;

public:
    TSecureEraseState() = default;

    [[nodiscard]] TVector<TString> GetDevicesToErase() const;
    [[nodiscard]] TVector<TRequestInfoPtr> GetRequests() const;
    [[nodiscard]] std::optional<NProto::TError> HandleRequest(
        const TString& deviceId,
        ui32 generation,
        ui64 idempotencyKey);

    [[nodiscard]] TSecureErase* Find(const TString& deviceId);
    [[nodiscard]] const TSecureErase* Find(const TString& deviceId) const;
    TSecureErase& GetOrAdd(const TString& deviceId);

    [[nodiscard]] bool IsEraseInProgress(const TString& deviceId) const;
    [[nodiscard]] bool CanStart(
        const TString& deviceId,
        const TString& deviceName,
        ui32 maxParallelSecureErases) const;

    // Rejects I/O while secure erase is waiting or in progress. A completed
    // secure erase record is removed before allowing I/O.
    [[nodiscard]] EIoApproveStatus ApproveIo(const TString& deviceId);

    void Start(const TString& deviceId, const TString& deviceName);
    TSecureErase& Complete(
        const TString& deviceId,
        const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
