#include "filesystem_counters.h"

#include <util/generic/hash.h>
#include <util/system/rwlock.h>
#include <util/system/types.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCountersEntry
{
    TDynamicCountersPtr Counters;
    TString CloudId;
    TString FolderId;
    ui64 RefCount = 0;
};

class TFsCountersProvider final: public IFsCountersProvider
{
private:
    TDynamicCountersPtr FsCounters;

    TRWMutex Lock;
    THashMap<std::pair<TString, TString>, TCountersEntry> CountersMap;

public:
    TFsCountersProvider(TString component, TDynamicCountersPtr rootCounters)
    {
        FsCounters = rootCounters->GetSubgroup("component", component + "_fs")
                         ->GetSubgroup("host", "cluster");
    }

    TDynamicCountersPtr Register(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId) override
    {
        auto key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);

        auto it = CountersMap.find(key);
        if (it != CountersMap.end()) {
            ++it->second.RefCount;
            return it->second.Counters;
        }

        auto counters = FsCounters->GetSubgroup("filesystem", fileSystemId)
                            ->GetSubgroup("client", clientId)
                            ->GetSubgroup("cloud", cloudId)
                            ->GetSubgroup("folder", folderId);

        CountersMap.emplace(
            key,
            TCountersEntry{counters, cloudId, folderId, 1});
        return counters;
    }

    void Unregister(
        const TString& fileSystemId,
        const TString& clientId) override
    {
        auto key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);

        auto it = CountersMap.find(key);
        if (it == CountersMap.end()) {
            return;
        }

        if (--it->second.RefCount > 0) {
            return;
        }

        CountersMap.erase(it);

        FsCounters->GetSubgroup("filesystem", fileSystemId)
            ->RemoveSubgroup("client", clientId);
    }

    void UpdateCloudAndFolder(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& newCloudId,
        const TString& newFolderId) override
    {
        auto key = std::make_pair(fileSystemId, clientId);

        TWriteGuard guard(Lock);

        auto it = CountersMap.find(key);
        if (it == CountersMap.end()) {
            return;
        }

        const auto& currentCloudId = it->second.CloudId;
        const auto& currentFolderId = it->second.FolderId;

        if ((!newCloudId || newCloudId == currentCloudId) &&
            (!newFolderId || newFolderId == currentFolderId))
        {
            return;
        }

        auto fsCounters = FsCounters->GetSubgroup("filesystem", fileSystemId)
                              ->GetSubgroup("client", clientId)
                              ->GetSubgroup("cloud", currentCloudId)
                              ->GetSubgroup("folder", currentFolderId);

        FsCounters->RemoveSubgroupChain(
            {{"filesystem", fileSystemId}, {"client", clientId}});

        FsCounters->GetSubgroup("filesystem", fileSystemId)
            ->GetSubgroup("client", clientId)
            ->GetSubgroup("cloud", newCloudId)
            ->GetSubgroup("folder", newFolderId);

        FsCounters->GetSubgroup("filesystem", fileSystemId)
            ->GetSubgroup("client", clientId)
            ->GetSubgroup("cloud", newCloudId)
            ->ReplaceSubgroup("folder", newFolderId, fsCounters);

        it->second.Counters = fsCounters;
        it->second.CloudId = newCloudId;
        it->second.FolderId = newFolderId;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFsCountersProviderStub final: public IFsCountersProvider
{
public:
    TDynamicCountersPtr Register(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
        return MakeIntrusive<TDynamicCounters>();
    }

    void Unregister(
        const TString& fileSystemId,
        const TString& clientId) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
    }

    void UpdateCloudAndFolder(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& newCloudId,
        const TString& newFolderId) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(clientId);
        Y_UNUSED(newCloudId);
        Y_UNUSED(newFolderId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFsCountersProviderPtr CreateFsCountersProvider(
    TString component,
    TDynamicCountersPtr rootCounters)
{
    return std::make_shared<TFsCountersProvider>(
        std::move(component),
        std::move(rootCounters));
}

IFsCountersProviderPtr CreateFsCountersProviderStub()
{
    return std::make_shared<TFsCountersProviderStub>();
}

}   // namespace NCloud::NFileStore
