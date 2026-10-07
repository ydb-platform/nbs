#include "device_generator.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>
#include <util/string/printf.h>

#include <library/cpp/digest/md5/md5.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TPathConfig = NProto::TStorageDiscoveryConfig::TPathConfig;
using TPoolConfig = NProto::TStorageDiscoveryConfig::TPoolConfig;

bool IsSuitablePool(const TPoolConfig& pool, ui64 fileSize)
{
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
}

ui32 GetBlockSize(
    const TPathConfig& pathConfig,
    const TPoolConfig& poolConfig,
    ui32 fileBlockSize)
{
    if (poolConfig.GetBlockSize()) {
        return poolConfig.GetBlockSize();
    }

    return pathConfig.GetBlockSize()
        ? pathConfig.GetBlockSize()
        : fileBlockSize;
}

ui32 GetMaxDeviceCount(
    const TPathConfig& pathConfig,
    const TPoolConfig& poolConfig,
    ui32 generatedDeviceCount)
{
    ui32 limit = pathConfig.GetMaxDeviceCount()
        ? pathConfig.GetMaxDeviceCount()
        : Max<ui32>();

    if (limit <= generatedDeviceCount) {
        return 0;
    }

    limit -= generatedDeviceCount;

    if (!poolConfig.GetMaxDeviceCount()) {
        return limit;
    }

    return Min(limit, poolConfig.GetMaxDeviceCount());
}

}   // namespace

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
    const bool sequentialLayout = pathConfig.GetSequentialLayout();

    //
    // Select the pools by the file size: all suitable pools for the
    // sequential layout, only the first one otherwise
    //

    TVector<const TPoolConfig*> pools;
    for (const auto& pool: pathConfig.GetPoolConfigs()) {
        if (!IsSuitablePool(pool, fileSize)) {
            continue;
        }

        pools.push_back(&pool);

        if (!sequentialLayout) {
            break;
        }
    }

    if (pools.empty()) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "unable to find the appropriate pool for " << path);
    }

    if (!sequentialLayout && !pools.front()->HasLayout()) {
        const auto& poolConfig = *pools.front();

        auto& file = Result.emplace_back();
        file.SetPath(path);
        file.SetBlockSize(GetBlockSize(pathConfig, poolConfig, fileBlockSize));
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

    //
    // Check the layouts before generating anything: a pool without a layout
    // would take the whole file, so it can't share the file with other pools
    //

    for (const auto* pool: pools) {
        if (!pool->GetLayout().GetDeviceSize()) {
            STORAGE_ERROR("Invalid layout for " << path << ":" << deviceNumber);

            return MakeError(E_ARGUMENT, "invalid layout");
        }
    }

    //
    // Lay out the devices of each pool right after the devices of the
    // previous one. The sub device index is shared by all pools of the file
    // to keep the device ids unique even if the pools have the same hash
    // suffix
    //

    ui64 offset = 0;
    ui32 subDeviceIndex = 0;

    for (const auto* pool: pools) {
        const auto& poolConfig = *pool;

        GenerateDevices(
            path,
            poolConfig,
            deviceNumber,
            GetBlockSize(pathConfig, poolConfig, fileBlockSize),
            GetMaxDeviceCount(pathConfig, poolConfig, subDeviceIndex),
            fileSize,
            offset,
            subDeviceIndex);
    }

    return {};
}

void TDeviceGenerator::GenerateDevices(
    const TString& path,
    const NProto::TStorageDiscoveryConfig::TPoolConfig& poolConfig,
    ui32 deviceNumber,
    ui32 blockSize,
    ui32 maxDeviceCount,
    ui64 fileSize,
    ui64& offset,
    ui32& subDeviceIndex)
{
    const auto& layout = poolConfig.GetLayout();

    ui64 deviceOffset = layout.GetHeaderSize();

    ui32 deviceCount = 0;
    while (deviceCount < maxDeviceCount &&
           offset + deviceOffset + layout.GetDeviceSize() <= fileSize)
    {
        auto& file = Result.emplace_back();
        file.SetPath(path);
        file.SetBlockSize(blockSize);
        file.SetPoolName(poolConfig.GetPoolName());
        if (poolConfig.HasJournalConfig()) {
            *file.MutableJournalConfig() = poolConfig.GetJournalConfig();
        }
        file.SetOffset(offset + deviceOffset);
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
        ++deviceCount;

        STORAGE_INFO("Found " << file);

        offset += deviceOffset + layout.GetDeviceSize();
        deviceOffset = layout.GetDevicePadding();
    }
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
