#include "volume_state.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/testlib/diagnostics.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 VolumeHistoryCacheSize = 100;

////////////////////////////////////////////////////////////////////////////////

NProto::TVolumeMeta CreateVolumeMeta(
    const NProto::TVolumePerformanceProfile& pp)
{
    NProto::TVolumeMeta meta;
    meta.MutableConfig()->MutablePerformanceProfile()->CopyFrom(pp);
    meta.MutableConfig()->SetBlockSize(4_KB);
    return meta;
}

NProto::TVolumeClientInfo CreateVolumeClientInfo(
    const TString& clientId,
    const NProto::EVolumeAccessMode accessMode,
    const NProto::EVolumeMountMode mountMode,
    ui32 seqNum = 0)
{
    NProto::TVolumeClientInfo info;
    info.SetClientId(clientId);
    info.SetVolumeAccessMode(accessMode);
    info.SetVolumeMountMode(mountMode);
    info.SetMountSeqNumber(seqNum);
    return info;
}

TThrottlerConfig CreateThrottlerConfig(
    TDuration maxThrottlerDelay = TDuration::Seconds(30),
    ui32 maxWriteCostMultiplier = 10,
    ui32 defaultPostponedRequestWeight = 1,
    TDuration initialBoostBudget = TDuration::Minutes(30),
    bool useDiskSpaceScore = false)
{
    return TThrottlerConfig(
        maxThrottlerDelay,
        maxWriteCostMultiplier,
        defaultPostponedRequestWeight,
        initialBoostBudget,
        useDiskSpaceScore);
}

TStorageConfigPtr MakeConfig(
    TDuration inactiveClientsTimeout,
    TDuration volumeHistoryDuration)
{
    NProto::TStorageServiceConfig config;
    config.SetInactiveClientsTimeout(inactiveClientsTimeout.MilliSeconds());
    config.SetVolumeHistoryDuration(volumeHistoryDuration.MilliSeconds());
    config.SetVolumeHistoryCacheSize(VolumeHistoryCacheSize);
    return std::make_shared<TStorageConfig>(
        std::move(config),
        std::make_shared<NFeatures::TFeaturesConfig>());
}

TVolumeState CreateVolumeState(
    TThrottlerConfig throttlerConfig,
    TDuration inactiveClientsTimeout = TDuration::Seconds(10),
    const NProto::TVolumePerformanceProfile& pp = {},
    THashMap<TString, TVolumeClientState> clientInfos = {},
    TVector<TCheckpointRequest> checkpointRequests = {})
{
    return TVolumeState(
        MakeConfig(inactiveClientsTimeout, {}),
        CreateDiagnosticsConfig(),
        CreateVolumeMeta(pp),
        {{TInstant::Seconds(100), CreateVolumeMeta(pp)}},   // metaHistory
        {},                                                 // volumeParams
        throttlerConfig,
        std::move(clientInfos),
        {},   // mountHistory
        std::move(checkpointRequests),
        {},     // followers
        {},     // leaders
        false   // startPartitionsNeeded
    );
}

TVolumeState CreateVolumeState(
    TDuration inactiveClientsTimeout = TDuration::Seconds(10),
    const NProto::TVolumePerformanceProfile& pp = {},
    THashMap<TString, TVolumeClientState> clientInfos = {},
    TVector<TCheckpointRequest> checkpointRequests = {})
{
    return TVolumeState(
        MakeConfig(inactiveClientsTimeout, {}),
        CreateDiagnosticsConfig(),
        CreateVolumeMeta(pp),
        {{TInstant::Seconds(100), CreateVolumeMeta(pp)}},   // metaHistory
        {},                                                 // volumeParams
        CreateThrottlerConfig(),
        std::move(clientInfos),
        TCachedVolumeMountHistory{VolumeHistoryCacheSize, {}},
        std::move(checkpointRequests),
        {},     // followers
        {},     // leaders
        false   // startPartitionsNeeded
    );
}

TVolumeState CreateVolumeState(
    const NProto::TStorageServiceConfig& config,
    TVolumeMountHistorySlice history)
{
    return {
        std::make_shared<TStorageConfig>(
            config,
            std::make_shared<NFeatures::TFeaturesConfig>()),
        CreateDiagnosticsConfig(),
        CreateVolumeMeta({}),
        {},   // metaHistory
        {},   // volumeParams
        CreateThrottlerConfig(),
        {},   // clientInfos
        TCachedVolumeMountHistory{
            config.GetVolumeHistoryCacheSize(),
            std::move(history)},
        {},     // checkpointRequests
        {},     // followers
        {},     // leaders
        false   // startPartitionsNeeded
    };
}

TActorId CreateActor(ui64 id)
{
    return TActorId(0, 0, id, 0);
}

TActorId CreateActor(ui32 nodeId, ui64 id)
{
    return TActorId(nodeId, 0, id, 0);
}

void CheckPipeState(
    const TVolumeClientState& client,
    TActorId actor,
    TVolumeClientState::EPipeState state)
{
    const auto& pipes = client.GetPipes();
    auto it = pipes.find(actor);
    UNIT_ASSERT_C(pipes.end() != it, "Pipe not found");
    UNIT_ASSERT_VALUES_EQUAL(
        state,
        it->second.State);
}

void CheckServicePipeRemoved(
    const TVolumeState& volumeState,
    const TString& clientId,
    TActorId serverId
)
{
    const auto& allPipes = volumeState.GetPipeServerId2ClientId();
    auto pipeIt = allPipes.find(serverId);
    UNIT_ASSERT_C(allPipes.end() == pipeIt, "server id is still registered");

    const auto& allClients = volumeState.GetClients();
    auto clientInfoIt = allClients.find(clientId);
    if (allClients.end() == clientInfoIt) {
        return;
    };

    const auto& pipes = clientInfoIt->second.GetPipes();
    auto it = pipes.find(serverId);
    if (it == pipes.end()) {
        return;
    }
    if (it->second.State != TVolumeClientState::EPipeState::DEACTIVATED
        && it->second.State != TVolumeClientState::EPipeState::WAIT_START)
    {
        UNIT_ASSERT_C(false, "Wrong pipe state");
    }
}

void CheckServicePipe(
    const TVolumeState& volumeState,
    TActorId serverId,
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode,
    TVolumeClientState::EPipeState pipeState)
{
    const auto& allPipes = volumeState.GetPipeServerId2ClientId();
    auto pipeIt = allPipes.find(serverId);
    UNIT_ASSERT_C(allPipes.end() != pipeIt, "server id is not registered");

    const auto& allClients = volumeState.GetClients();
    auto clientInfoIt = allClients.find(pipeIt->second);
    UNIT_ASSERT_C(allClients.end() != clientInfoIt, "client not found");

    auto it = clientInfoIt->second.GetPipes().find(serverId);
    UNIT_ASSERT_C(clientInfoIt->second.GetPipes().end() != it, "server id not found");

    const auto& pipeInfo = it->second;
    UNIT_ASSERT_VALUES_EQUAL(
        accessMode,
        clientInfoIt->second.GetVolumeClientInfo().GetVolumeAccessMode());
    UNIT_ASSERT_VALUES_EQUAL(mountMode, pipeInfo.MountMode);
    UNIT_ASSERT_VALUES_EQUAL(pipeState, pipeInfo.State);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeStateTest)
{
    Y_UNIT_TEST(ShouldNotRemoveNotAddedClient)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();

        auto result = volumeState.RemoveClient(clientId, TActorId());
        UNIT_ASSERT_VALUES_EQUAL(result.GetCode(), S_ALREADY);
    }

    Y_UNIT_TEST(ShouldAddClient)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();

        auto info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        auto res = volumeState.AddClient(info, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);
    }

    Y_UNIT_TEST(ShouldNotAddSameClientTwice)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();

        auto info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        auto res = volumeState.AddClient(info, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        res = volumeState.AddClient(info, CreateActor(1));
        UNIT_ASSERT_EQUAL(res.Error.GetCode(), S_ALREADY);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        auto& allClients = volumeState.AccessClients();
        const auto& clientInfo = allClients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(1, clientInfo.GetPipes().size());
    }

    Y_UNIT_TEST(ShouldNotAddClientWithEmptyClientId)
    {
        auto volumeState = CreateVolumeState();
        TString clientId;

        auto info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        auto res = volumeState.AddClient(info);
        UNIT_ASSERT(res.Error.GetCode() == E_ARGUMENT);
    }

    Y_UNIT_TEST(ShouldRemovePreviouslyAddedClient)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();

        auto info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        auto res = volumeState.AddClient(info, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        auto result = volumeState.RemoveClient(clientId, CreateActor(1));
        UNIT_ASSERT_C(!FAILED(result.GetCode()), result);
        CheckServicePipeRemoved(
            volumeState,
            clientId,
            CreateActor(1));

        const auto& allClients = volumeState.GetClients();
        UNIT_ASSERT_VALUES_EQUAL(true, allClients.end() == allClients.find(clientId));

        CheckServicePipeRemoved(
            volumeState,
            clientId,
            CreateActor(1));
    }

    Y_UNIT_TEST(ShouldNotRemoveClientWithEmptyId)
    {
        auto volumeState = CreateVolumeState();
        auto result = volumeState.RemoveClient(TString(), TActorId());
        UNIT_ASSERT(result.GetCode() == E_ARGUMENT);
    }

    Y_UNIT_TEST(ShouldDisallowMultipleAddClientWithReadWriteAccess)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();

        auto info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        auto res = volumeState.AddClient(info, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        auto clientId2 = CreateGuidAsString();
        info = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
        res = volumeState.AddClient(info, CreateActor(2));
        UNIT_ASSERT_EQUAL(res.Error.GetCode(), E_BS_MOUNT_CONFLICT);

        CheckServicePipeRemoved(
            volumeState,
            clientId2,
            CreateActor(2));
    }

    Y_UNIT_TEST(ShouldAllowMultipleAddClientWithSingleLocalMount)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();

        auto info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);
        auto res = volumeState.AddClient(info, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        auto clientId2 = CreateGuidAsString();

        info = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);
        res = volumeState.AddClient(info, CreateActor(2));
        UNIT_ASSERT_EQUAL(res.Error.GetCode(), E_BS_MOUNT_CONFLICT);

        CheckServicePipeRemoved(
            volumeState,
            clientId2,
            CreateActor(2));

        info = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        res = volumeState.AddClient(info, CreateActor(3));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(3),
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        {
            auto result = volumeState.RemoveClient(clientId, CreateActor(1));
            UNIT_ASSERT_C(!FAILED(result.GetCode()), result);

            CheckServicePipeRemoved(
                volumeState,
                clientId,
                CreateActor(1));
        }

        {
            auto result = volumeState.RemoveClient(clientId2, TActorId());
            UNIT_ASSERT_C(!FAILED(result.GetCode()), result);

            CheckServicePipeRemoved(
                volumeState,
                clientId2,
                CreateActor(3));
        }

        info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        res = volumeState.AddClient(info, CreateActor(4));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(4),
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        info = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        res = volumeState.AddClient(info, CreateActor(5));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(5),
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        auto clientId3 = CreateGuidAsString();
        info = CreateVolumeClientInfo(
            clientId3,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);
        res = volumeState.AddClient(info, CreateActor(6));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(5),
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        auto clientId4 = CreateGuidAsString();
        info = CreateVolumeClientInfo(
            clientId4,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);
        res = volumeState.AddClient(info, CreateActor(7));
        UNIT_ASSERT(FAILED(res.Error.GetCode()));

        CheckServicePipeRemoved(
            volumeState,
            clientId4,
            CreateActor(7));

        info = CreateVolumeClientInfo(
            clientId4,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        res = volumeState.AddClient(info, CreateActor(8));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(8),
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);
    }

    Y_UNIT_TEST(ShouldAllowMultipleAddClientWithSingleReadWriteAccess)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();

        auto info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        auto res = volumeState.AddClient(info, CreateActor(1));
        UNIT_ASSERT_C(SUCCEEDED(res.Error.GetCode()), res.Error.GetMessage());

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        auto clientId2 = CreateGuidAsString();
        info = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        res = volumeState.AddClient(info, CreateActor(2));
        UNIT_ASSERT_C(SUCCEEDED(res.Error.GetCode()), res.Error.GetMessage());

        CheckServicePipe(
            volumeState,
            CreateActor(2),
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);
    }

    Y_UNIT_TEST(ShouldConsiderTimedOutClientsInactive)
    {
        TDuration inactiveClientsTimeout = TDuration::Seconds(10);

        auto volumeState = CreateVolumeState(inactiveClientsTimeout);

        auto clientId = CreateGuidAsString();
        auto info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        auto serverActorId = TActorId(0, "actor1");
        TInstant now = TInstant::Now();

        auto res = volumeState.AddClient(
            info,
            serverActorId,
            serverActorId,
            now);
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);
        UNIT_ASSERT(!res.RemovedClientIds);

        CheckServicePipe(
            volumeState,
            serverActorId,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        auto* clientInfo = volumeState.GetClient(clientId);
        UNIT_ASSERT(clientInfo);

        auto secondClientId = CreateGuidAsString();
        auto secondInfo = CreateVolumeClientInfo(
            secondClientId,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);
        auto secondServerActorId = TActorId(0, "actor2");

        volumeState.SetServiceDisconnected(serverActorId, now);

        CheckServicePipeRemoved(
            volumeState,
            clientId,
            serverActorId);

        // Should not be able to add the second client immediately
        // after the first client is disconnected
        res = volumeState.AddClient(
            secondInfo,
            secondServerActorId,
            secondServerActorId,
            now);
        UNIT_ASSERT(FAILED(res.Error.GetCode()));
        UNIT_ASSERT(!res.RemovedClientIds);

        CheckServicePipeRemoved(
            volumeState,
            clientId,
            secondServerActorId);

        TInstant later = now + inactiveClientsTimeout;

        // Should be able to add the second client after the first disconnected
        // one is timed out
        res = volumeState.AddClient(
            secondInfo,
            secondServerActorId,
            secondServerActorId,
            later);
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);
        UNIT_ASSERT_VALUES_EQUAL(1, res.RemovedClientIds.size());
        UNIT_ASSERT_VALUES_EQUAL(clientId, res.RemovedClientIds[0]);

        CheckServicePipe(
            volumeState,
            secondServerActorId,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);
    }

    Y_UNIT_TEST(ShouldResetDisconnectTimeWhenServerReconnects)
    {
        TDuration inactiveClientsTimeout = TDuration::Seconds(10);

        auto volumeState = CreateVolumeState(inactiveClientsTimeout);

        auto clientId = CreateGuidAsString();
        auto info = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        auto serverActorId = TActorId(0, "actor1");
        TInstant now = TInstant::Now();

        auto res = volumeState.AddClient(
            info,
            serverActorId,
            serverActorId,
            now);
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);
        UNIT_ASSERT(!res.RemovedClientIds);

        CheckServicePipe(
            volumeState,
            serverActorId,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        TInstant disconnectTime = now + TDuration::Seconds(1);

        volumeState.SetServiceDisconnected(serverActorId, disconnectTime);

        CheckServicePipeRemoved(
            volumeState,
            clientId,
            serverActorId);

        const auto& clients = volumeState.GetClients();
        auto* clientInfo = clients.FindPtr(clientId);
        auto time = clientInfo->GetVolumeClientInfo().GetDisconnectTimestamp();
        UNIT_ASSERT(time == disconnectTime.MicroSeconds());

        res = volumeState.AddClient(
            info,
            serverActorId,
            serverActorId,
            now);
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            serverActorId,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        UNIT_ASSERT(!clientInfo->GetVolumeClientInfo().GetDisconnectTimestamp());
    }

    Y_UNIT_TEST(TestResetMeta)
    {
        NProto::TVolumePerformanceProfile pp;
        pp.SetMaxReadBandwidth(1);
        auto volumeState = CreateVolumeState(
            CreateThrottlerConfig(),
            {},
            pp);
        pp.SetMaxReadBandwidth(2);
        auto meta = CreateVolumeMeta(pp);

        volumeState.ResetMeta(meta);
        const auto& tp = volumeState.GetThrottlingPolicy();
        const auto& config = tp.GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(2, config.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EStorageAccessMode::Default),
            static_cast<int>(volumeState.GetStorageAccessMode())
        );
        UNIT_ASSERT(!volumeState.GetMuteIOErrors());

        {
            *meta.MutableVolumeConfig()->MutableTagsStr() =
                "aaa,repair,bbb,xxx,mute-io-errors,zzz";
            volumeState.ResetMeta(meta);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(EStorageAccessMode::Repair),
                static_cast<int>(volumeState.GetStorageAccessMode())
            );
            UNIT_ASSERT(volumeState.GetMuteIOErrors());
        }

        {
            *meta.MutableVolumeConfig()->MutableTagsStr() = "repair";
            volumeState.ResetMeta(meta);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(EStorageAccessMode::Repair),
                static_cast<int>(volumeState.GetStorageAccessMode())
            );
        }

        {
            *meta.MutableVolumeConfig()->MutableTagsStr() = "";
            volumeState.ResetMeta(meta);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(EStorageAccessMode::Default),
                static_cast<int>(volumeState.GetStorageAccessMode())
            );
        }

        {
            *meta.MutableVolumeConfig()->MutableTagsStr() = "aaa,bbb";
            volumeState.ResetMeta(meta);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(EStorageAccessMode::Default),
                static_cast<int>(volumeState.GetStorageAccessMode())
            );
        }
        // TODO: test other TVolumeState fields?
    }

    Y_UNIT_TEST(ShouldFillThrottlerConfigUponConstructAndReset)
    {
        NProto::TVolumePerformanceProfile pp;
        pp.SetBoostPercentage(200);
        pp.SetBoostTime(15'000);
        pp.SetMaxReadBandwidth(1);
        auto volumeState = CreateVolumeState(
            CreateThrottlerConfig(
                TDuration::Seconds(30),   // maxThrottlerDelay
                10,                       // maxWriteCostMultiplier
                1,                        // defaultPostponedRequestWeight
                CalculateBoostTime(pp)    // startupBoostBudget
            ),
            {},
            pp);

        {
            const auto& tp = volumeState.GetThrottlingPolicy();
            UNIT_ASSERT_VALUES_EQUAL(15'000, tp.GetCurrentBoostBudget().MilliSeconds());
        }

        pp.SetMaxReadBandwidth(2);
        auto meta = CreateVolumeMeta(pp);
        volumeState.ResetMeta(meta);
        {
            const auto& tp = volumeState.GetThrottlingPolicy();
            UNIT_ASSERT_VALUES_EQUAL(15'000, tp.GetCurrentBoostBudget().MilliSeconds());
        }

        volumeState.Reset();
        {
            const auto& tp = volumeState.GetThrottlingPolicy();
            UNIT_ASSERT_VALUES_EQUAL(15'000, tp.GetCurrentBoostBudget().MilliSeconds());
        }
    }

    Y_UNIT_TEST(ShouldFillThrottlerConfigWithCustomInitialBudgetUponConstructAndReset)
    {
        NProto::TVolumePerformanceProfile pp;
        pp.SetMaxReadBandwidth(1);
        auto throttlerConfig = CreateThrottlerConfig(
            TDuration::Seconds(30),   // maxThrottlerDelay
            10,                       // maxWriteCostMultiplier
            1,                        // defaultPostponedRequestWeight
            TDuration::Seconds(4)     // startupBoostBudget
        );
        auto volumeState = CreateVolumeState(
            throttlerConfig,
            {},
            pp);

        {
            const auto& tp = volumeState.GetThrottlingPolicy();
            UNIT_ASSERT_VALUES_EQUAL(4'000, tp.GetCurrentBoostBudget().MilliSeconds());
        }

        pp.SetMaxReadBandwidth(2);
        auto meta = CreateVolumeMeta(pp);
        volumeState.ResetMeta(meta);
        {
            const auto& tp = volumeState.GetThrottlingPolicy();
            UNIT_ASSERT_VALUES_EQUAL(4'000, tp.GetCurrentBoostBudget().MilliSeconds());
        }

        volumeState.Reset();
        {
            const auto& tp = volumeState.GetThrottlingPolicy();
            UNIT_ASSERT_VALUES_EQUAL(4'000, tp.GetCurrentBoostBudget().MilliSeconds());
        }
    }

    Y_UNIT_TEST(ShouldNotAllowSecondLocalClientWhenLocalRW)
    {
        auto volumeState = CreateVolumeState();
        auto clientId1 = CreateGuidAsString();
        auto clientId2 = CreateGuidAsString();

        auto info1 = CreateVolumeClientInfo(
            clientId1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        auto res = volumeState.AddClient(info1, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        res = volumeState.AddClient(info2, CreateActor(2));
        UNIT_ASSERT_C(HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipeRemoved(
            volumeState,
            clientId2,
            CreateActor(2));
    }

    Y_UNIT_TEST(ShouldNotAllowSecondLocalReadOnlyWhenLocalRO)
    {
        auto volumeState = CreateVolumeState();
        auto clientId1 = CreateGuidAsString();
        auto clientId2 = CreateGuidAsString();

        auto info1 = CreateVolumeClientInfo(
            clientId1,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        auto res = volumeState.AddClient(info1, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        res = volumeState.AddClient(info2, CreateActor(2));
        UNIT_ASSERT_C(HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipeRemoved(
            volumeState,
            clientId2,
            CreateActor(2));
    }

    Y_UNIT_TEST(ShouldAllowNewLRWWithHigherSeqNum)
    {
        auto volumeState = CreateVolumeState();
        auto clientId1 = CreateGuidAsString();
        auto clientId2 = CreateGuidAsString();

        auto info1 = CreateVolumeClientInfo(
            clientId1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            1);

        auto res = volumeState.AddClient(info1, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        UNIT_ASSERT_VALUES_EQUAL(0, res.RemovedClientIds.size());

        res = volumeState.AddClient(info2, CreateActor(2));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        UNIT_ASSERT_VALUES_EQUAL(1, res.RemovedClientIds.size());
        UNIT_ASSERT_VALUES_EQUAL(clientId1, res.RemovedClientIds[0]);

        CheckServicePipe(
            volumeState,
            CreateActor(2),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);
    }

    Y_UNIT_TEST(ShouldAllowNewLRWWithHigherSeqNumAndOldRRW)
    {
        auto volumeState = CreateVolumeState();
        auto clientId1 = CreateGuidAsString();
        auto clientId2 = CreateGuidAsString();

        auto info1 = CreateVolumeClientInfo(
            clientId1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto info2 = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            1);

        auto res = volumeState.AddClient(info1, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        UNIT_ASSERT_VALUES_EQUAL(0, res.RemovedClientIds.size());

        res = volumeState.AddClient(info2, CreateActor(2));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        UNIT_ASSERT_VALUES_EQUAL(1, res.RemovedClientIds.size());
        UNIT_ASSERT_VALUES_EQUAL(clientId1, res.RemovedClientIds[0]);

        CheckServicePipe(
            volumeState,
            CreateActor(2),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);
    }

    Y_UNIT_TEST(ShouldAllowLRWIfLROAlreadyExists)
    {
        auto volumeState = CreateVolumeState();
        auto clientId1 = CreateGuidAsString();
        auto clientId2 = CreateGuidAsString();

        auto info1 = CreateVolumeClientInfo(
            clientId1,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto res = volumeState.AddClient(info1, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        res = volumeState.AddClient(info2, CreateActor(2));
        UNIT_ASSERT_C(HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipeRemoved(
            volumeState,
            clientId2,
            CreateActor(2));
    }

    Y_UNIT_TEST(ShouldNotAllowLROIfLRWAlreadyExists)
    {
        auto volumeState = CreateVolumeState();
        auto clientId1 = CreateGuidAsString();
        auto clientId2 = CreateGuidAsString();

        auto info1 = CreateVolumeClientInfo(
            clientId1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId2,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        auto res = volumeState.AddClient(info1, CreateActor(1));
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        res = volumeState.AddClient(info2, CreateActor(2));
        UNIT_ASSERT_C(HasError(res.Error), res.Error);

        CheckServicePipe(
            volumeState,
            CreateActor(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipeRemoved(
            volumeState,
            clientId2,
            CreateActor(2));
    }

    Y_UNIT_TEST(ShouldCacheLimitedNumberOfRecords)
    {
        auto volumeState = CreateVolumeState();

        for (size_t i = 0; i < VolumeHistoryCacheSize + 10; ++i) {
            auto clientId = CreateGuidAsString();

            auto info1 = CreateVolumeClientInfo(
                clientId,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);

            auto res = volumeState.LogAddClient(
                TInstant::Now(),
                info1,
                MakeError(S_OK), {}, {});
            const auto& history = volumeState.GetMountHistory().GetItems();
            if (history.size() >= 2) {
                UNIT_ASSERT(
                    res.Key.Timestamp != history[1].Key.Timestamp ||
                    res.Key.SeqNo != history[1].Key.SeqNo);
            }
            UNIT_ASSERT(history.size() <= VolumeHistoryCacheSize);
            UNIT_ASSERT(history.size() > 0);
        }
    }

    Y_UNIT_TEST(ShouldNotCrashWhenCleanupEmptyHistory)
    {
        auto volumeState = CreateVolumeState();
        volumeState.AccessMountHistory().CleanupHistoryIfNeeded(TInstant::Now());
    }

    Y_UNIT_TEST(ShouldCheckStaleWhenCalledHasActiveClients)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto pipeServerActorId = TActorId(10, 10);

        auto res = volumeState.AddClient(info1, pipeServerActorId);
        UNIT_ASSERT_C(!HasError(res.Error), res.Error);

        UNIT_ASSERT(volumeState.HasActiveClients(TInstant::Now()));

        volumeState.SetServiceDisconnected(pipeServerActorId, TInstant::Now());

        CheckServicePipeRemoved(
            volumeState,
            clientId,
            pipeServerActorId);

        UNIT_ASSERT(
            !volumeState.HasActiveClients(TInstant::Now() + TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(ShouldAllowAdditionalConnectionFromTheSameClient)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();
        auto actor1 = CreateActor(1);
        auto actor2 = CreateActor(2);

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto res = volumeState.AddClient(info1, actor1);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        res = volumeState.AddClient(info2, actor2);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        auto& clients = volumeState.AccessClients();
        UNIT_ASSERT_VALUES_EQUAL(1, clients.size());

        const auto& client = clients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

        CheckServicePipe(
            volumeState,
            actor1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipe(
            volumeState,
            actor2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_ACCESS_READ_WRITE,
            client.GetVolumeClientInfo().GetVolumeAccessMode());

        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_MOUNT_LOCAL,
            client.GetVolumeClientInfo().GetVolumeMountMode());
    }

    Y_UNIT_TEST(ShouldPreserveRepairIfAdditionalConnectionFromTheSameClient)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();
        auto actor1 = CreateActor(1);
        auto actor2 = CreateActor(2);

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_REPAIR,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto res = volumeState.AddClient(info1, actor1);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        res = volumeState.AddClient(info2, actor2);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        auto& clients = volumeState.AccessClients();
        UNIT_ASSERT_VALUES_EQUAL(1, clients.size());

        const auto& client = clients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

        CheckServicePipe(
            volumeState,
            actor1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipe(
            volumeState,
            actor2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);
    }

    Y_UNIT_TEST(ShouldHandlePipeDisconnect)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();
        auto actor1 = CreateActor(1);
        auto actor2 = CreateActor(2);

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto res = volumeState.AddClient(info1, actor1);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        res = volumeState.AddClient(info2, actor2);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        TInstant now = TInstant::Now();
        volumeState.SetServiceDisconnected(actor2, now);

        auto& clients = volumeState.AccessClients();
        UNIT_ASSERT_VALUES_EQUAL(1, clients.size());

        const auto& client = clients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());

        CheckServicePipe(
            volumeState,
            actor1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipeRemoved(
            volumeState,
            clientId,
            actor2);

        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_ACCESS_READ_WRITE,
            client.GetVolumeClientInfo().GetVolumeAccessMode());

        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_MOUNT_LOCAL,
            client.GetVolumeClientInfo().GetVolumeMountMode());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            client.GetVolumeClientInfo().GetDisconnectTimestamp());
    }

    Y_UNIT_TEST(ShouldSetDisconnectTimestampIfAllPipesAreDisconnected)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();
        auto actor1 = CreateActor(1);
        auto actor2 = CreateActor(2);

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto res = volumeState.AddClient(info1, actor1);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        res = volumeState.AddClient(info2, actor2);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        TInstant now;
        volumeState.SetServiceDisconnected(actor2, now);

        CheckServicePipeRemoved(
            volumeState,
            clientId,
            actor2);

        now += TDuration::Seconds(10);

        volumeState.SetServiceDisconnected(actor1, now);

        CheckServicePipeRemoved(
            volumeState,
            clientId,
            actor1);

        auto& clients = volumeState.AccessClients();
        UNIT_ASSERT_VALUES_EQUAL(1, clients.size());

        const auto& client = clients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(0, client.GetPipes().size());

        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_ACCESS_READ_WRITE,
            client.GetVolumeClientInfo().GetVolumeAccessMode());

        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_MOUNT_REMOTE,
            client.GetVolumeClientInfo().GetVolumeMountMode());

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Seconds(10).MicroSeconds(),
            client.GetVolumeClientInfo().GetDisconnectTimestamp());
    }

    Y_UNIT_TEST(ShouldActivateLocalDataPathAndRejectOtherPaths)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();
        auto serverActor1 = CreateActor(1, 1);
        auto serverActor2 = CreateActor(2, 2);

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto res = volumeState.AddClient(info1, serverActor1, serverActor1);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        res = volumeState.AddClient(info2, serverActor2, serverActor2);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        auto& clients = volumeState.AccessClients();
        UNIT_ASSERT_VALUES_EQUAL(1, clients.size());

        auto& client = clients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

        CheckServicePipe(
            volumeState,
            serverActor1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipe(
            volumeState,
            serverActor2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        auto result = client.CheckLocalRequest(serverActor1.NodeId(), true, "", "");
        UNIT_ASSERT_C(!FAILED(result.GetCode()), result);

        CheckPipeState(
            client,
            serverActor1,
            TVolumeClientState::EPipeState::ACTIVE);

        result = client.CheckLocalRequest(serverActor1.NodeId(), true, "", "");
        UNIT_ASSERT_C(!FAILED(result.GetCode()), result);

        result = client.CheckPipeRequest(serverActor2, true, "", "");
        UNIT_ASSERT_C(!FAILED(result.GetCode()), result);

        result = client.CheckLocalRequest(serverActor1.NodeId(), true, "", "");
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, result.GetCode());

        CheckPipeState(
            client,
            serverActor1,
            TVolumeClientState::EPipeState::DEACTIVATED);
        CheckPipeState(
            client,
            serverActor2,
            TVolumeClientState::EPipeState::ACTIVE);
    }

    Y_UNIT_TEST(ShouldActivateRemoteDataPathAndRejectOtherPaths)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();
        auto serverActor1 = CreateActor(1);
        auto serverActor2 = CreateActor(2);

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto res = volumeState.AddClient(info1, serverActor1);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        res = volumeState.AddClient(info2, serverActor2);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        auto& clients = volumeState.AccessClients();
        UNIT_ASSERT_VALUES_EQUAL(1, clients.size());

        auto& client = clients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

        CheckServicePipe(
            volumeState,
            serverActor1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipe(
            volumeState,
            serverActor2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        auto result = client.CheckPipeRequest(serverActor2, true, "", "");
        UNIT_ASSERT_C(!FAILED(result.GetCode()), result);

        CheckPipeState(
            client,
            serverActor2,
            TVolumeClientState::EPipeState::ACTIVE);

        result = client.CheckPipeRequest(serverActor2, true, "", "");
        UNIT_ASSERT_C(!FAILED(result.GetCode()), result);

        result = client.CheckLocalRequest(serverActor1.NodeId(), true, "", "");
        UNIT_ASSERT_C(!FAILED(result.GetCode()), result);

        result = client.CheckPipeRequest(serverActor2, true, "", "");

        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, result.GetCode());

        CheckPipeState(
            client,
            serverActor2,
            TVolumeClientState::EPipeState::DEACTIVATED);

        CheckPipeState(
            client,
            serverActor1,
            TVolumeClientState::EPipeState::ACTIVE);
    }

    Y_UNIT_TEST(ShouldRemoveClientFromSpecificPipe)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();
        auto serverActor1 = CreateActor(1);
        auto serverActor2 = CreateActor(2);

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto res = volumeState.AddClient(info1, serverActor1);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        res = volumeState.AddClient(info2, serverActor2);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        auto& clients = volumeState.AccessClients();
        UNIT_ASSERT_VALUES_EQUAL(1, clients.size());

        auto& client = clients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

        CheckServicePipe(
            volumeState,
            serverActor1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipe(
            volumeState,
            serverActor2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        {
            auto res = volumeState.RemoveClient(clientId, serverActor2);
            UNIT_ASSERT_C(!FAILED(res.GetCode()), res);

            CheckServicePipeRemoved(
                volumeState,
                clientId,
                serverActor2);

            auto& client = clients[clientId];
            UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());
        }
    }

    Y_UNIT_TEST(ShouldRemoveClientInCaseOfMultiplePipes)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();
        auto serverActor1 = CreateActor(1);
        auto serverActor2 = CreateActor(2);

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto res = volumeState.AddClient(info1, serverActor1);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        res = volumeState.AddClient(info2, serverActor2);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        auto& clients = volumeState.AccessClients();
        UNIT_ASSERT_VALUES_EQUAL(1, clients.size());

        auto& client = clients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

        CheckServicePipe(
            volumeState,
            serverActor1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipe(
            volumeState,
            serverActor2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        {
            auto res = volumeState.RemoveClient(clientId, TActorId());
            UNIT_ASSERT_C(!FAILED(res.GetCode()), res);
        }
    }

    Y_UNIT_TEST(ShouldRemoveClientIfRemoveClientAndNoPipesRemain)
    {
        auto volumeState = CreateVolumeState();
        auto clientId = CreateGuidAsString();
        auto serverActor1 = CreateActor(1);
        auto serverActor2 = CreateActor(2);

        auto info1 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto info2 = CreateVolumeClientInfo(
            clientId,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        auto res = volumeState.AddClient(info1, serverActor1);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        res = volumeState.AddClient(info2, serverActor2);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error);

        auto& clients = volumeState.AccessClients();
        UNIT_ASSERT_VALUES_EQUAL(1, clients.size());

        auto& client = clients[clientId];
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

        CheckServicePipe(
            volumeState,
            serverActor1,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            TVolumeClientState::EPipeState::WAIT_START);

        CheckServicePipe(
            volumeState,
            serverActor2,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            TVolumeClientState::EPipeState::WAIT_START);

        {
            auto res = volumeState.RemoveClient(clientId, serverActor2);
            UNIT_ASSERT_C(!FAILED(res.GetCode()), res);
            CheckServicePipeRemoved(
                volumeState,
                clientId,
                serverActor2);
        }

        {
            auto res = volumeState.RemoveClient(clientId, serverActor1);
            UNIT_ASSERT_C(!FAILED(res.GetCode()), res);
            CheckServicePipeRemoved(
                volumeState,
                clientId,
                serverActor1);
        }

        auto it = clients.find(clientId);
        UNIT_ASSERT_C(clients.end() == it, "Client was not removed");
    }

    Y_UNIT_TEST(TestStartPartitionsNeeded)
    {
        auto volumeState = CreateVolumeState();

        UNIT_ASSERT(!volumeState.GetShouldStartPartitionsForGc(TInstant::Now()));

        volumeState.SetStartPartitionsNeeded(true);
        UNIT_ASSERT(volumeState.GetShouldStartPartitionsForGc(TInstant::Now()));

        volumeState.Reset();
        UNIT_ASSERT(volumeState.GetShouldStartPartitionsForGc(TInstant::Now()));
    }

    Y_UNIT_TEST(ShouldFilterFreshDeviceIdsForOldDisks)
    {
        // testing that we filter out garbage fresh device ids for the disks
        // created before NBS-4383 got fixed - roughly the end of August 2023
        auto volumeState = CreateVolumeState();
        auto meta = volumeState.GetMeta();
        const TInstant oldDate = TInstant::ParseIso8601("2023-08-30");
        meta.MutableVolumeConfig()->SetCreationTs(oldDate.MicroSeconds());
        meta.AddDevices()->SetDeviceUUID("d1");
        meta.AddDevices()->SetDeviceUUID("d2");
        auto& r1 = *meta.AddReplicas();
        r1.AddDevices()->SetDeviceUUID("d3");
        r1.AddDevices()->SetDeviceUUID("d4");
        auto& r2 = *meta.AddReplicas();
        r2.AddDevices()->SetDeviceUUID("d5");
        r2.AddDevices()->SetDeviceUUID("d6");
        meta.AddFreshDeviceIds("d2");
        meta.AddFreshDeviceIds("d3");
        meta.AddFreshDeviceIds("d5");
        meta.AddFreshDeviceIds("garbage1");
        meta.AddFreshDeviceIds("garbage2");
        meta.AddFreshDeviceIds("garbage3");
        volumeState.ResetMeta(meta);

        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TString>({"d2", "d3", "d5"}),
            TVector<TString>(
                volumeState.GetFilteredFreshDevices().begin(),
                volumeState.GetFilteredFreshDevices().end()));
    }

    Y_UNIT_TEST(ShouldNotFilterFreshDeviceIdsForNewDisks)
    {
        // testing that we do not filter out garbage fresh device ids for the
        // disks created after NBS-4383 got fixed
        auto volumeState = CreateVolumeState();
        auto meta = volumeState.GetMeta();
        const TInstant newDate = TInstant::ParseIso8601("2023-08-31");
        meta.MutableVolumeConfig()->SetCreationTs(newDate.MicroSeconds());
        meta.AddDevices()->SetDeviceUUID("d1");
        meta.AddDevices()->SetDeviceUUID("d2");
        auto& r1 = *meta.AddReplicas();
        r1.AddDevices()->SetDeviceUUID("d3");
        r1.AddDevices()->SetDeviceUUID("d4");
        auto& r2 = *meta.AddReplicas();
        r2.AddDevices()->SetDeviceUUID("d5");
        r2.AddDevices()->SetDeviceUUID("d6");
        meta.AddFreshDeviceIds("d2");
        meta.AddFreshDeviceIds("d3");
        meta.AddFreshDeviceIds("d5");
        meta.AddFreshDeviceIds("garbage1");
        meta.AddFreshDeviceIds("garbage2");
        meta.AddFreshDeviceIds("garbage3");
        volumeState.ResetMeta(meta);

        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TString>({
                "d2",
                "d3",
                "d5",
                "garbage1",
                "garbage2",
                "garbage3"}),
            TVector<TString>(
                volumeState.GetFilteredFreshDevices().begin(),
                volumeState.GetFilteredFreshDevices().end()));
    }

    Y_UNIT_TEST(ShouldRemoveOutdatedItemsFromCachedHistory)
    {
        NProto::TStorageServiceConfig config;
        config.SetVolumeHistoryDuration(TDuration::Seconds(1).MilliSeconds());
        config.SetVolumeHistoryCacheSize(100);

        TVolumeMountHistorySlice history{
            {
                {THistoryLogKey(TInstant::FromValue(10), 0), {}},
                {THistoryLogKey(TInstant::FromValue(9), 0), {}},
                {THistoryLogKey(TInstant::FromValue(8), 0), {}}
            },
            {}
        };

        auto volumeState = CreateVolumeState(
            config,
            std::move(history)
        );

        volumeState.AccessMountHistory().CleanupHistoryIfNeeded(TInstant::FromValue(8));
        UNIT_ASSERT_VALUES_EQUAL(2, volumeState.GetMountHistory().GetItems().size());

        volumeState.AccessMountHistory().CleanupHistoryIfNeeded(TInstant::FromValue(9));
        UNIT_ASSERT_VALUES_EQUAL(1, volumeState.GetMountHistory().GetItems().size());

        volumeState.AccessMountHistory().CleanupHistoryIfNeeded(TInstant::FromValue(30));
        UNIT_ASSERT_VALUES_EQUAL(0, volumeState.GetMountHistory().GetItems().size());
    }

    Y_UNIT_TEST(ShouldNotExceedCacheSizeWhenAddingHistoryRecords)
    {
        NProto::TStorageServiceConfig config;
        config.SetVolumeHistoryCacheSize(4);

        TVolumeMountHistorySlice history {
            {},
            {}
        };

        auto volumeState = CreateVolumeState(config, std::move(history));

        volumeState.LogAddClient(TInstant::FromValue(0), {}, {}, {}, {});
        volumeState.LogAddClient(TInstant::FromValue(1), {}, {}, {}, {});
        volumeState.LogAddClient(TInstant::FromValue(2), {}, {}, {}, {});
        volumeState.LogRemoveClient(TInstant::FromValue(3), {}, {}, {});
        volumeState.LogRemoveClient(TInstant::FromValue(4), {}, {}, {});

        UNIT_ASSERT_VALUES_EQUAL(4, volumeState.GetMountHistory().GetItems().size());
    }

    Y_UNIT_TEST(ShouldProperlyAllocateLogRecordsWithSameTimestamp)
    {
        NProto::TStorageServiceConfig config;
        config.SetVolumeHistoryCacheSize(3);

        TVolumeMountHistorySlice history{
            {
                {THistoryLogKey(TInstant::FromValue(10), 0), {}},
                {THistoryLogKey(TInstant::FromValue(8), 1), {}},
                {THistoryLogKey(TInstant::FromValue(8), 0), {}}
            },
            {}
        };

        auto volumeState = CreateVolumeState(
            config,
            std::move(history)
        );

        volumeState.LogAddClient(TInstant::FromValue(10), {}, {}, {}, {});

        const auto& h = volumeState.GetMountHistory();
        UNIT_ASSERT_VALUES_EQUAL(3, h.GetItems().size());
        auto key = h.GetItems().front().Key;
        UNIT_ASSERT_VALUES_EQUAL(TInstant::FromValue(10), key.Timestamp);
        UNIT_ASSERT_VALUES_EQUAL(1, key.SeqNo);
    }

    Y_UNIT_TEST(ShouldTrackRecordBeyondCache)
    {
        NProto::TStorageServiceConfig config;
        config.SetVolumeHistoryDuration(TDuration::Seconds(1).MilliSeconds());
        config.SetVolumeHistoryCacheSize(3);

        TVolumeMountHistorySlice history{
            {
                {THistoryLogKey(TInstant::FromValue(10), 0), {}},
                {THistoryLogKey(TInstant::FromValue(8), 1), {}},
                {THistoryLogKey(TInstant::FromValue(8), 0), {}}
            },
            {}
        };

        auto volumeState = CreateVolumeState(
            config,
            std::move(history)
        );

        UNIT_ASSERT_C(
            !volumeState.GetMountHistory().GetNextOlderRecord().has_value(),
            "RecordBeyondCache should not be set");

        volumeState.LogAddClient(TInstant::FromValue(11), {}, {}, {}, {});

        UNIT_ASSERT_C(
            volumeState.GetMountHistory().GetNextOlderRecord().has_value(),
            "RecordBeyondCache should be set");

        UNIT_ASSERT_VALUES_EQUAL(
            *volumeState.GetMountHistory().GetNextOlderRecord(),
            THistoryLogKey(TInstant::FromValue(8), 0));

        volumeState.AccessMountHistory().CleanupHistoryIfNeeded(TInstant::FromValue(9));
        UNIT_ASSERT_VALUES_EQUAL(2, volumeState.GetMountHistory().GetItems().size());

        UNIT_ASSERT_C(
            !volumeState.GetMountHistory().GetNextOlderRecord().has_value(),
            "RecordBeyondCache should not be set");
    }

    Y_UNIT_TEST(ShouldAdjustCachedHistoryOnStartupBasedOnConfig)
    {
        NProto::TStorageServiceConfig config;
        config.SetVolumeHistoryCacheSize(3);

        TVolumeMountHistorySlice history{
            {
                {THistoryLogKey(TInstant::FromValue(11), 0), {}},
                {THistoryLogKey(TInstant::FromValue(10), 0), {}},
                {THistoryLogKey(TInstant::FromValue(8), 1), {}},
                {THistoryLogKey(TInstant::FromValue(8), 0), {}}
            },
            {}
        };

        auto volumeState = CreateVolumeState(
            config,
            history);

        UNIT_ASSERT_C(
            volumeState.GetMountHistory().GetNextOlderRecord().has_value(),
            "RecordBeyondCache should be set");

        UNIT_ASSERT_VALUES_EQUAL(
            THistoryLogKey(TInstant::FromValue(8), 0),
            *volumeState.GetMountHistory().GetNextOlderRecord());


        auto slice = volumeState.GetMountHistory().GetSlice();
        UNIT_ASSERT_VALUES_EQUAL(
            slice.Items.size(),
            volumeState.GetMountHistory().GetItems().size());

        for (ui32 i = 0; i < slice.Items.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                slice.Items[i].Key,
                volumeState.GetMountHistory().GetItems()[i].Key);
        }
    }

    Y_UNIT_TEST(ShouldNotTrackUsedBlocks)
    {
        auto makeState = [] (auto kind, auto mode) {
            NProto::TVolumeMeta meta;

            auto& config = *meta.MutableConfig();
            config.SetStorageMediaKind(kind);

            auto& volumeConfig = *meta.MutableVolumeConfig();
            auto& part = *volumeConfig.AddPartitions();
            part.SetBlockCount(1);

            auto& desc = *volumeConfig.MutableEncryptionDesc();
            desc.SetMode(mode);

            return TVolumeState{
                MakeConfig(10s, 0s),
                CreateDiagnosticsConfig(),
                std::move(meta),
                {},     // metaHistory
                {},     // volumeParams
                CreateThrottlerConfig(),
                {},     // infos
                {},     // mountHistory
                {},     // checkpointRequests
                {},     // followers
                {},     // leaders
                false   // startPartitionsNeeded
            };
        };

        {
            auto state = makeState(
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                NProto::ENCRYPTION_AT_REST);

            UNIT_ASSERT(!state.GetTrackUsedBlocks());
        }

        {
            auto state = makeState(
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                NProto::NO_ENCRYPTION);

            UNIT_ASSERT(!state.GetTrackUsedBlocks());
        }

        {
            auto state = makeState(
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                NProto::ENCRYPTION_AES_XTS);

            UNIT_ASSERT(!state.GetTrackUsedBlocks());
        }

        {
            auto state = makeState(
                NProto::STORAGE_MEDIA_SSD,
                NProto::ENCRYPTION_AES_XTS);

            UNIT_ASSERT(!state.GetTrackUsedBlocks());
        }
    }

    Y_UNIT_TEST(AcquireDisk)
    {
        auto volumeState = CreateVolumeState();
        auto meta = volumeState.GetMeta();
        const TInstant oldDate = TInstant::ParseIso8601("2023-08-30");
        meta.MutableVolumeConfig()->SetCreationTs(oldDate.MicroSeconds());
        meta.AddDevices()->SetDeviceUUID("d1");
        meta.AddDevices()->SetDeviceUUID("d2");
        auto& r1 = *meta.AddReplicas();
        r1.AddDevices()->SetDeviceUUID("d3");
        r1.AddDevices()->SetDeviceUUID("d4");
        auto& r2 = *meta.AddReplicas();
        r2.AddDevices()->SetDeviceUUID("d5");
        r2.AddDevices()->SetDeviceUUID("d6");

        NProto::TDeviceMigration deviceMigration;
        deviceMigration.SetSourceDeviceId("d1");
        deviceMigration.MutableTargetDevice()->SetDeviceUUID("d7");

        meta.MutableMigrations()->Add(std::move(deviceMigration));
        volumeState.ResetMeta(meta);

        const THashSet<TString>
            deviceUUIDSExpected{"d1", "d2", "d3", "d4", "d5", "d6", "d7"};

        THashSet<TString> devicesUUIDSActual;
        for (const auto& d: volumeState.GetDevicesForAcquireOrRelease()) {
            devicesUUIDSActual.insert(d.GetDeviceUUID());
        }

        UNIT_ASSERT_EQUAL(deviceUUIDSExpected, devicesUUIDSActual);
    }

    Y_UNIT_TEST(FollowerDisks)
    {
        auto volumeState = CreateVolumeState();

        // Add link uuid1 for volO -> vol1
        volumeState.AddOrUpdateFollower(TFollowerDiskInfo{
            .Link =
                TLeaderFollowerLink{
                    .LinkUUID = "uuid1",
                    .LeaderDiskId = "vol0",
                    .LeaderShardId = "su0",
                    .FollowerDiskId = "vol1",
                    .FollowerShardId = "su1"},
            .State = TFollowerDiskInfo::EState::None,
            .MediaKind = NProto::STORAGE_MEDIA_SSD});

        const auto& followers = volumeState.GetAllFollowers();
        UNIT_ASSERT_VALUES_EQUAL(1, followers.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid1", followers[0].Link.LinkUUID);
        UNIT_ASSERT_VALUES_EQUAL("vol1", followers[0].Link.FollowerDiskId);
        UNIT_ASSERT_EQUAL(TFollowerDiskInfo::EState::None, followers[0].State);
        UNIT_ASSERT_EQUAL(std::nullopt, followers[0].MigratedBytes);

        // Update link uuid1
        volumeState.AddOrUpdateFollower(TFollowerDiskInfo{
            .Link =
                TLeaderFollowerLink{
                    .LinkUUID = "uuid1",
                    .LeaderDiskId = "vol0",
                    .LeaderShardId = "su0",
                    .FollowerDiskId = "vol1",
                    .FollowerShardId = "su1"},
            .State = TFollowerDiskInfo::EState::Preparing,
            .MediaKind = NProto::STORAGE_MEDIA_SSD,
            .MigratedBytes = 100});
        UNIT_ASSERT_VALUES_EQUAL(1, followers.size());
        UNIT_ASSERT_EQUAL(
            TFollowerDiskInfo::EState::Preparing,
            followers[0].State);
        UNIT_ASSERT_VALUES_EQUAL(100, *followers[0].MigratedBytes);

        // Add link uuid2 volO -> vol2
        volumeState.AddOrUpdateFollower(TFollowerDiskInfo{
            .Link =
                TLeaderFollowerLink{
                    .LinkUUID = "uuid2",
                    .LeaderDiskId = "vol0",
                    .LeaderShardId = "su0",
                    .FollowerDiskId = "vol2",
                    .FollowerShardId = "su1"},
            .State = TFollowerDiskInfo::EState::None,
            .MediaKind = NProto::STORAGE_MEDIA_SSD});
        UNIT_ASSERT_VALUES_EQUAL(2, followers.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid2", followers[1].Link.LinkUUID);
        UNIT_ASSERT_VALUES_EQUAL("vol2", followers[1].Link.FollowerDiskId);

        // Remove link uuid1 by linkUUID
        volumeState.RemoveFollower(TLeaderFollowerLink{.LinkUUID = "uuid1"});
        UNIT_ASSERT_VALUES_EQUAL(1, followers.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid2", followers[0].Link.LinkUUID);

        // Remove link uuid2 by leader and follower diskID
        volumeState.RemoveFollower(TLeaderFollowerLink{
            .LinkUUID = "",
            .LeaderDiskId = "vol0",
            .LeaderShardId = "su0",
            .FollowerDiskId = "vol2",
            .FollowerShardId = "su1"});
        UNIT_ASSERT_VALUES_EQUAL(0, followers.size());
    }
}

}   // namespace NCloud::NBlockStore::NStorage

template <>
inline void Out<NCloud::NBlockStore::NStorage::TVolumeClientState::EPipeState>(
    IOutputStream& out,
    NCloud::NBlockStore::NStorage::TVolumeClientState::EPipeState state)
{
    out << (ui32)state;
}

template <>
inline void Out<NCloud::NBlockStore::NProto::EVolumeMountMode>(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EVolumeMountMode mountMode)
{
    out << (ui32)mountMode;
}

template <>
inline void Out<NCloud::NBlockStore::NProto::EVolumeAccessMode>(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EVolumeAccessMode accessMode)
{
    out << (ui32)accessMode;
}
