#include "device_generator.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>
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
    const NProto::TStorageDiscoveryConfig::TPathConfig& pathConfig,
    ui32 deviceNumber,
    ui32 fileBlockSize,
    ui64 fileSize)
{
    auto* pool = FindIfPtr(pathConfig.GetPoolConfigs(), [&] (const auto& pool) {
        ui64 minSize = pool.GetMinSize();

        if (!minSize && pool.HasLayout()) {
            minSize =
                pool.GetLayout().GetHeaderSize() +
                pool.GetLayout().GetDeviceSize();
        }

        const ui64 maxSize = pool.GetMaxSize()
            ? pool.GetMaxSize()
            : fileSize;

        return minSize <= fileSize && fileSize <= maxSize;
    });

    if (!pool) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "unable to find the appropriate pool for " << path);
    }

    const auto& poolConfig = *pool;

    const ui32 blockSize = poolConfig.GetBlockSize()
        ? poolConfig.GetBlockSize()
        : pathConfig.GetBlockSize()
            ? pathConfig.GetBlockSize()
            : fileBlockSize;

    const ui32 maxDeviceCount = poolConfig.GetMaxDeviceCount()
        ? poolConfig.GetMaxDeviceCount()
        : pathConfig.GetMaxDeviceCount();

    if (!poolConfig.HasLayout()) {
        auto& file = Result.emplace_back();
        file.SetPath(path);
        file.SetBlockSize(blockSize);
        file.SetPoolName(poolConfig.GetPoolName());
        if (poolConfig.HasJournalConfig()) {
            *file.MutableJournalConfig() = poolConfig.GetJournalConfig();
        }
        switch (poolConfig.GetHashScheme()) {
            case NProto::TStorageDiscoveryConfig::HS_LEGACY:
                file.SetDeviceId(
                    CreateDeviceId(deviceNumber, poolConfig.GetHashSuffix()));
                break;
            case NProto::TStorageDiscoveryConfig::HS_FULL_PATH:
                file.SetDeviceId(
                    CreateDeviceId(path, poolConfig.GetHashSuffix()));
                break;
        }

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
        if (poolConfig.HasJournalConfig()) {
            *file.MutableJournalConfig() = poolConfig.GetJournalConfig();
        }
        file.SetOffset(offset);
        file.SetFileSize(layout.GetDeviceSize());

        switch (poolConfig.GetHashScheme()) {
            case NProto::TStorageDiscoveryConfig::HS_LEGACY:
                file.SetDeviceId(CreateDeviceId(
                    deviceNumber,
                    poolConfig.GetHashSuffix(),
                    subDeviceIndex));
                break;
            case NProto::TStorageDiscoveryConfig::HS_FULL_PATH:
                file.SetDeviceId(CreateDeviceId(
                    path,
                    poolConfig.GetHashSuffix(),
                    subDeviceIndex));
                break;
        }

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

TString TDeviceGenerator::CreateDeviceId(
    const TString& path,
    const TString& suffix,
    ui32 subDeviceIndex) const
{
    const auto s = Sprintf(
        "%s-%s-%03u%s",
        AgentId.c_str(),
        path.c_str(),
        subDeviceIndex + 1,
        suffix.c_str());

    return MD5::Calc(s);
}

TString TDeviceGenerator::CreateDeviceId(
    const TString& path,
    const TString& suffix) const
{
    const auto s = Sprintf(
        "%s-%s-%s",
        AgentId.c_str(),
        path.c_str(),
        suffix.c_str());

    return MD5::Calc(s);
}

}   // namespace NCloud::NBlockStore::NStorage
