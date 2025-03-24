#include "device_generator.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/hash_set.h>
#include <util/string/printf.h>

#include <library/cpp/digest/md5/md5.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDeviceGenerator::TDeviceGenerator(TLog log, TString agentId)
    : Log(std::move(log))
    , AgentId(std::move(agentId))
{}

NProto::TError TDeviceGenerator::operator () (
    const TString& path,
    const NProto::TStorageDiscoveryConfig::TPoolConfig& poolConfig,
    ui32 deviceNumber,
    ui32 maxDeviceCount,
    ui32 blockSize,
    ui64 fileSize)
{
    if (!poolConfig.HasLayout()) {
        auto& file = Result.emplace_back();
        file.SetPath(path);
        file.SetBlockSize(blockSize);
        file.SetPoolName(poolConfig.GetPoolName());
        file.SetDeviceId(CreateDeviceId(deviceNumber, poolConfig.GetHashSuffix()));

        STORAGE_INFO("Found " << file);

        return {};
    }

    const auto& layout = poolConfig.GetLayout();

    if (!layout.GetDeviceSize()) {
        STORAGE_ERROR("Invalid layout for " << path << ":" << deviceNumber);

        return MakeError(E_ARGUMENT, "invalid layout");
    }

    ui64 offset = layout.GetHeaderSize();

    ui32 subDeviceIndex = 0;
    while (offset + layout.GetDeviceSize() <= fileSize) {
        auto& file = Result.emplace_back();
        file.SetPath(path);
        file.SetBlockSize(blockSize);
        file.SetPoolName(poolConfig.GetPoolName());
        file.SetOffset(offset);
        file.SetFileSize(layout.GetDeviceSize());
        file.SetDeviceId(CreateDeviceId(
            deviceNumber,
            poolConfig.GetHashSuffix(),
            subDeviceIndex));

        ++subDeviceIndex;

        STORAGE_INFO("Found " << file);

        offset += layout.GetDeviceSize() + layout.GetDevicePadding();

        if (maxDeviceCount && subDeviceIndex >= maxDeviceCount) {
            break;
        }
    }

    return {};
}

TVector<NProto::TFileDeviceArgs> TDeviceGenerator::ExtractResult()
{
    TVector<NProto::TFileDeviceArgs> tmp;
    tmp.swap(Result);

    return tmp;
}

void TDeviceGenerator::AddPossibleUUIDSForDevice(
    const NProto::TStorageDiscoveryConfig::TPoolConfig& poolConfig,
    ui32 deviceNumber,
    ui32 maxDeviceCount,
    THashSet<TString>& setToAddUUIDs)
{
    setToAddUUIDs.emplace(
        CreateDeviceId(deviceNumber, poolConfig.GetHashSuffix()));
    for (ui32 i = 0; i < maxDeviceCount; ++i) {
        setToAddUUIDs.emplace(
            CreateDeviceId(deviceNumber, poolConfig.GetHashSuffix(), i));
    }
}

TString TDeviceGenerator::CreateDeviceId(
    ui32 deviceNumber,
    const TString& suffix,
    ui32 subDeviceIndex) const
{
    const auto s = Sprintf(
        "%s-%02u-%03u%s",
        AgentId.c_str(),
        deviceNumber,
        subDeviceIndex + 1,
        suffix.c_str());

    return MD5::Calc(s);
}

TString TDeviceGenerator::CreateDeviceId(
    ui32 deviceNumber,
    const TString& suffix) const
{
    const auto s = Sprintf(
        "%s-%02u%s",
        AgentId.c_str(),
        deviceNumber,
        suffix.c_str());

    return MD5::Calc(s);
}

}   // namespace NCloud::NBlockStore::NStorage
