#include "compare_configs.h"

#include <util/string/builder.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError CompareConfigs(
    const NProto::TFileDeviceArgs& expected,
    const NProto::TFileDeviceArgs& current,
    bool checkSerialNumber = false)
{
    if (expected.GetPath() != current.GetPath()) {
        return MakeError(E_ARGUMENT, "Unexpected path");
    }

    if (expected.GetPoolName() != current.GetPoolName()) {
        return MakeError(E_ARGUMENT, "Unexpected pool name");
    }

    if (expected.GetBlockSize() != current.GetBlockSize()) {
        return MakeError(E_ARGUMENT, "Unexpected block size");
    }

    if (expected.GetOffset() != current.GetOffset()) {
        return MakeError(E_ARGUMENT, "Unexpected offset");
    }

    if (expected.GetFileSize() && expected.GetFileSize() != current.GetFileSize()) {
        return MakeError(E_ARGUMENT, "Unexpected file size");
    }

    if (checkSerialNumber && expected.GetSerialNumber() != current.GetSerialNumber()) {
        return MakeError(E_ARGUMENT, "Unexpected serial number");
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError CompareConfigs(
    const TVector<NProto::TFileDeviceArgs>& expectedConfig,
    const TVector<NProto::TFileDeviceArgs>& currentConfig,
    bool strictCompare)
{
    auto byId = [] (const auto& device) -> TStringBuf {
        return device.GetDeviceId();
    };

    if (!IsSortedBy(expectedConfig,  byId)) {
        return MakeError(E_ARGUMENT, "expected config is not sorted");
    }

    if (!IsSortedBy(currentConfig,  byId)) {
        return MakeError(E_ARGUMENT, "current config is not sorted");
    }

    auto pathExists = [&] (const auto& device) {
        return NFs::Exists(device.GetPath());
    };

    size_t i = 0;
    size_t j = 0;

    while (i != expectedConfig.size() && j != currentConfig.size()) {
        const auto& expected = expectedConfig[i];
        const auto& current = currentConfig[j];

        if (expected.GetDeviceId() > current.GetDeviceId()) {
            // new device

            if (strictCompare) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "Device " << expected << " has been added");
            }

            ++j;
            continue;
        }

        if (expected.GetDeviceId() == current.GetDeviceId()) {
            const auto error =
                CompareConfigs(expected, current, strictCompare);
            if (HasError(error)) {
                return MakeError(error.GetCode(), TStringBuilder()
                    << error.GetMessage() << ". Expected config: "
                    << expected << ". Current config: " << current);
            }

            ++i;
            ++j;

            continue;
        }

        if (pathExists(expected) || strictCompare) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Device " << expected << " has been lost");
        }

        ++i;
    }

    if (strictCompare &&
        (expectedConfig.begin() + i != expectedConfig.end()))
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Devices has been lost");
    }

    const auto* lostDevice = FindIfPtr(
        expectedConfig.begin() + i,
        expectedConfig.end(),
        pathExists);

    if (lostDevice) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Device " << *lostDevice << " has been lost");
    }

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
