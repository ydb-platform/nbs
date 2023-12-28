#include "compare_configs.h"

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError CompareConfigs(
    const NProto::TFileDeviceArgs& expected,
    const NProto::TFileDeviceArgs& current)
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

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError CompareConfigs(
    const TVector<NProto::TFileDeviceArgs>& expectedConfig,
    const TVector<NProto::TFileDeviceArgs>& currentConfig)
{
    auto byId = [] (const auto& lhs, const auto& rhs) {
        return lhs.GetDeviceId() < rhs.GetDeviceId();
    };

    if (!std::is_sorted(
        expectedConfig.begin(),
        expectedConfig.end(),
        byId))
    {
        return MakeError(E_ARGUMENT, "expected config is not sorted");
    }

    if (!std::is_sorted(
        currentConfig.begin(),
        currentConfig.end(),
        byId))
    {
        return MakeError(E_ARGUMENT, "current config is not sorted");
    }

    size_t i = 0;
    size_t j = 0;

    while (i != expectedConfig.size() && j != currentConfig.size()) {
        const auto& expected = expectedConfig[i];
        const auto& current = currentConfig[j];

        if (expected.GetDeviceId() > current.GetDeviceId()) {
            // new device
            ++j;
            continue;
        }

        if (expected.GetDeviceId() == current.GetDeviceId()) {
            const auto error = CompareConfigs(expected, current);
            if (HasError(error)) {
                return MakeError(error.GetCode(), TStringBuilder()
                    << error.GetMessage() << ". Expected config: "
                    << expected << ". Current config: " << current);
            }

            ++i;
            ++j;

            continue;
        }

        return MakeError(E_ARGUMENT, TStringBuilder()
            << "Device " << expected << " has been lost");
    }

    if (i != expectedConfig.size()) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "Device " << expectedConfig[i] << " has been lost");
    }

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
