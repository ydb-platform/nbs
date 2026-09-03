#include "secure_erase_state.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

const THashMap<TString, TSecureErase>&
TSecureEraseState::GetSecureErases() const
{
    return SecureErases;
}

TSecureErase* TSecureEraseState::Find(const TString& deviceId)
{
    return SecureErases.FindPtr(deviceId);
}

const TSecureErase* TSecureEraseState::Find(const TString& deviceId) const
{
    return SecureErases.FindPtr(deviceId);
}

TSecureErase& TSecureEraseState::GetOrAdd(const TString& deviceId)
{
    return SecureErases[deviceId];
}

bool TSecureEraseState::IsInProgress(const TString& deviceId) const
{
    return DevicesInProgress.contains(deviceId);
}

bool TSecureEraseState::CanStart(const TString& deviceId,
                                 const TString& deviceName,
                                 ui32 maxParallelSecureErases) const
{
    const auto* erase = Find(deviceId);
    return erase && erase->Status == ESecureEraseStatus::Wait &&
           !DevicesInProgress.contains(deviceId) &&
           !DevicesNamesInProgress.contains(deviceName) &&
           DevicesInProgress.size() < maxParallelSecureErases;
}

void TSecureEraseState::Start(const TString& deviceId,
                              const TString& deviceName)
{
    auto* erase = Find(deviceId);
    Y_ABORT_UNLESS(erase);
    Y_ABORT_UNLESS(erase->Status == ESecureEraseStatus::Wait);

    const auto deviceInsertResult = DevicesInProgress.insert(deviceId);
    Y_ABORT_UNLESS(deviceInsertResult.second);

    const auto deviceNameInsertResult =
        DevicesNamesInProgress.insert(deviceName);
    Y_ABORT_UNLESS(deviceNameInsertResult.second);

    erase->DeviceName = deviceName;
    erase->Status = ESecureEraseStatus::InProgress;
}

TSecureErase& TSecureEraseState::Complete(const TString& deviceId,
                                          const NProto::TError& error)
{
    auto* erase = Find(deviceId);
    Y_ABORT_UNLESS(erase);
    Y_ABORT_UNLESS(erase->Status == ESecureEraseStatus::InProgress);

    DevicesInProgress.erase(deviceId);
    DevicesNamesInProgress.erase(erase->DeviceName);

    erase->Status = ESecureEraseStatus::Completed;
    erase->Error = error;
    return *erase;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
