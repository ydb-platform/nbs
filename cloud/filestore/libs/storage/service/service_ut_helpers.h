#pragma once

#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NFileStore::NStorage {

class TServiceClient;
class TTestEnv;

class TTestProfileLog
    : public IProfileLog
{
public:
    TMap<ui32, TVector<TRecord>> Requests;

    void Start() override;

    void Stop() override;

    void Write(TRecord record) override;

    void RegisterCounters(NMonitoring::TDynamicCounters& root) override;
};

// Helper class to count Describe/Create/Alter/Destroy/Configure
// shard requests. We intentionally skip Main FS requests and requests
// made by proxies. That's because we want to process only original shard
// requests to check max in-flight and request/response counters.
class TShardRequestCounter
{
private:
    NActors::TTestActorRuntimeBase& Runtime;
    const TString ShardIdPrefix;
    NActors::TTestActorRuntimeBase::TEventFilter PrevFilter;
    THashMap<ui64, NActors::TActorId> DescribeRequestSenders;
    THashMap<ui64, NActors::TActorId> ConfigureRequestSenders;

public:
    ui32 DescribeRequests = 0;
    ui32 DescribeResponses = 0;
    ui32 DescribeMaxInFlight = 0;
    ui32 CreateRequests = 0;
    ui32 CreateResponses = 0;
    ui32 CreateMaxInFlight = 0;
    ui32 ConfigureRequests = 0;
    ui32 ConfigureResponses = 0;
    ui32 ConfigureMaxInFlight = 0;
    ui32 AlterRequests = 0;
    ui32 AlterResponses = 0;
    ui32 AlterMaxInFlight = 0;
    ui32 DestroyRequests = 0;
    ui32 DestroyResponses = 0;
    ui32 DestroyMaxInFlight = 0;

    TShardRequestCounter(NActors::TTestActorRuntimeBase& runtime, const TString& fsId)
        : Runtime(runtime)
        , ShardIdPrefix(TStringBuilder() << fsId << ShardNumPrefix)
    {
        PrevFilter = Runtime.SetEventFilter(
            [this](auto& runtime, TAutoPtr<NActors::IEventHandle>& event)
            {
                CountEvent(event);
                return PrevFilter ? PrevFilter(runtime, event) : false;
            });
    }

    ~TShardRequestCounter()
    {
        Runtime.SetEventFilter(PrevFilter);
    }

private:
    void CountEvent(const TAutoPtr<NActors::IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            case TEvSSProxy::EvCreateFileStoreRequest: {
                if (++CreateRequests > CreateResponses) {
                    CreateMaxInFlight = std::max(
                        CreateMaxInFlight,
                        CreateRequests - CreateResponses);
                }
                break;
            }

            case TEvSSProxy::EvCreateFileStoreResponse: {
                ++CreateResponses;
                break;
            }

            case TEvIndexTablet::EvConfigureAsShardRequest: {
                // Skipping calls made by Proxies.
                if (ev->Recipient != MakeIndexTabletProxyServiceId()) {
                    return;
                }
                ConfigureRequestSenders[ev->Cookie] = ev->Sender;
                if (++ConfigureRequests > ConfigureResponses) {
                    ConfigureMaxInFlight = std::max(
                        ConfigureMaxInFlight,
                        ConfigureRequests - ConfigureResponses);
                }
                break;
            }

            case TEvIndexTablet::EvConfigureAsShardResponse: {
                // We count only calls made by original actor.
                auto it = ConfigureRequestSenders.find(ev->Cookie);
                if (it == ConfigureRequestSenders.end() ||
                    ev->Recipient != it->second)
                {
                    return;
                }
                ConfigureRequestSenders.erase(it);
                ++ConfigureResponses;
                break;
            }

            case TEvSSProxy::EvDescribeFileStoreRequest: {
                using TRequest = TEvSSProxy::TEvDescribeFileStoreRequest;
                const auto* msg = ev->Get<TRequest>();
                // We need to count only DescribeShard operations, so excluding:
                // 1. Cookie == Max<ui64>(): this is MainFileStoreCookie called
                // from TAlterFileStoreActor::DescribeMainFileStore.
                // 2. ShardIdPrefix we also need to check, as there is a call
                // TIndexTabletProxyActor::DescribeFileStore with Cookie
                // set to conn.Id.
                if (ev->Cookie == Max<ui64>() ||
                    !msg->FileSystemId.StartsWith(ShardIdPrefix))
                {
                    return;
                }
                DescribeRequestSenders[ev->Cookie] = ev->Sender;
                if (++DescribeRequests > DescribeResponses) {
                    DescribeMaxInFlight = std::max(
                        DescribeMaxInFlight,
                        DescribeRequests - DescribeResponses);
                }
                break;
            }

            case TEvSSProxy::EvDescribeFileStoreResponse: {
                // Skipping this Cookie, it's MainFileStoreCookie called from
                // TAlterFileStoreActor::DescribeMainFileStore.
                if (ev->Cookie == Max<ui64>()) {
                    return;
                }
                // Skipping calls made by TIndexTabletProxyActor.
                auto it = DescribeRequestSenders.find(ev->Cookie);
                if (it == DescribeRequestSenders.end() ||
                    ev->Recipient != it->second)
                {
                    return;
                }
                ++DescribeResponses;
                break;
            }

            case TEvSSProxy::EvAlterFileStoreRequest: {
                // Skipping MainFileStoreCookie.
                if (ev->Cookie == Max<ui64>()) {
                    return;
                }
                if (++AlterRequests > AlterResponses) {
                    AlterMaxInFlight = std::max(
                        AlterMaxInFlight,
                        AlterRequests - AlterResponses);
                }
                break;
            }

            case TEvSSProxy::EvAlterFileStoreResponse: {
                // Skipping MainFileStoreCookie.
                if (ev->Cookie == Max<ui64>()) {
                    return;
                }
                ++AlterResponses;
                break;
            }

            case TEvSSProxy::EvDestroyFileStoreRequest: {
                if (++DestroyRequests > DestroyResponses) {
                    DestroyMaxInFlight = std::max(
                        DestroyMaxInFlight,
                        DestroyRequests - DestroyResponses);
                }
                break;
            }

            case TEvSSProxy::EvDestroyFileStoreResponse: {
                ++DestroyResponses;
                break;
            }
        }
    }
};

TString GenerateValidateData(ui32 size, ui32 seed = 0);

void WaitForTabletStart(TServiceClient& service);

NProtoPrivate::TGetStorageStatsResponse GetStorageStats(
    TServiceClient& service,
    const TString& fsId,
    const ui64 cacheTTL = 0,
    const NProtoPrivate::EStatsRequestMode mode =
        NProtoPrivate::STATS_REQUEST_MODE_DEFAULT);

void CreateOrResizeFilesystem(
    TServiceClient& service,
    const TString& fsId,
    ui64 fsBlocksCount,
    bool resize,
    TMap<TString, NActors::TActorId>& fsToActor);

void UpdateCounters(
    TTestEnv& env,
    TServiceClient& service,
    ui32 nodeIdx,
    NActors::TActorId fsActorId);

inline NProto::TStorageConfig MakeStorageConfig()
{
    NProto::TStorageConfig config;
    return config;
}

inline NProto::TStorageConfig MakeStorageConfigWithDirectoryCreationInShards()
{
    NProto::TStorageConfig config;
    config.SetDirectoryCreationInShardsEnabled(true);
    return config;
}

}   // namespace NCloud::NFileStore::NStorage
