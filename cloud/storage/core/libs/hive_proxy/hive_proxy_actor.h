#pragma once

#include "public.h"

#include "hive_proxy_events_private.h"

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/core/base/hive.h>
#include <contrib/ydb/core/mind/local.h>
#include <contrib/ydb/core/tablet/tablet_pipe_client_cache.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

class THiveProxyActor final
    : public NActors::TActorBootstrapped<THiveProxyActor>
{
public:
    struct TRequestInfo
    {
        NActors::TActorId Sender;
        ui64 Cookie = 0;

        TRequestInfo() = default;
        TRequestInfo(const TRequestInfo&) = default;
        TRequestInfo& operator=(const TRequestInfo&) = default;

        TRequestInfo(NActors::TActorId sender, ui64 cookie)
            : Sender(sender)
            , Cookie(cookie)
        {
        }

        void Drop()
        {
            Sender = {};
            Cookie = 0;
        }

        explicit operator bool() const
        {
            return !!Sender;
        }
    };

    struct TCreateOrLookupRequest
        : public TRequestInfo
    {
        const bool IsLookup = false;
        TAutoPtr<NActors::IEventHandle> Event;

        TCreateOrLookupRequest(bool isLookup, TAutoPtr<NActors::IEventHandle> event)
            : TRequestInfo(event->Sender, event->Cookie)
            , IsLookup(isLookup)
            , Event(std::move(event))
        {}
    };

private:
    enum ELockPhase
    {
        PHASE_LOCKING,
        PHASE_LOCKED,
        PHASE_RECONNECT,
        PHASE_UNLOCKING,
    };

    struct TLockState
    {
        NActors::TActorId Owner;
        ui64 Cookie = 0;
        ui64 TabletId = 0;
        ELockPhase Phase = PHASE_LOCKING;
        TRequestInfo LockRequest;
        TRequestInfo UnlockRequest;
    };

    class TTabletStats
    {
        enum StatsUpdateState
        {
            // Previous stats where sent and acknowledged.
            // No new stats for hive have arrived
            STATE_NO_STATS,
            // Previous stats where sent and acknowledged.
            // New stats for hive have arrived
            STATE_HAS_STATS,
            // Stats have been sent, but not yet acknowledged.
            STATE_SENDING_STATS,
            // Stats have been sent, but not yet acknowledged.
            // But new stats from tablet have arrived. After ack from hive
            // new stats need to be sent again.
            STATE_SEND_UPDATE
        };

    private:
        StatsUpdateState State = STATE_NO_STATS;

    public:
        NKikimrTabletBase::TMetrics ResourceValues;
        ui32 SlaveID = 0;

        void OnStatsUpdate()
        {
            switch (State) {
                case STATE_NO_STATS: State = STATE_HAS_STATS; break;
                case STATE_HAS_STATS: break;
                default: State = STATE_SEND_UPDATE; break;
            }
        }

        void OnStatsSend(bool resend)
        {
            Y_DEBUG_ABORT_UNLESS(State != STATE_NO_STATS);
            Y_DEBUG_ABORT_UNLESS(resend || State != STATE_SENDING_STATS);
            if (State == STATE_HAS_STATS) {
                State = STATE_SENDING_STATS;
            }
        }

        void OnHiveAck()
        {
            Y_DEBUG_ABORT_UNLESS(State != STATE_NO_STATS);
            Y_DEBUG_ABORT_UNLESS(State != STATE_HAS_STATS);
            switch (State) {
                case STATE_SENDING_STATS: State = STATE_NO_STATS; break;
                case STATE_SEND_UPDATE: State = STATE_HAS_STATS; break;
                default: break;
            }
        }

        bool IsEmpty() const
        {
            return State == STATE_NO_STATS;
        }
    };

    using TGetInfoRequests = TDeque<TRequestInfo>;

    using TTabletKey = std::pair<ui64, ui64>;

    using TCreateOrLookupRequestQueue = TDeque<TCreateOrLookupRequest>;

    struct THiveState
    {
        THashMap<ui64, TLockState> LockStates;
        THashMap<ui64, TGetInfoRequests> GetInfoRequests;
        THashMap<TTabletKey, TCreateOrLookupRequestQueue> CreateRequests;
        THashSet<NActors::TActorId> Actors;
        THashMap<ui64, TTabletStats> UpdatedTabletMetrics;
        bool ScheduledSendTabletMetrics = false;
    };

private:
    static constexpr TDuration BatchTimeout = TDuration::Seconds(2);

    std::unique_ptr<NKikimr::NTabletPipe::IClientCache> ClientCache;
    THashMap<ui64, THiveState> HiveStates;

    const TDuration LockExpireTimeout;
    const int LogComponent;

    TString TabletBootInfoBackupFilePath;
    bool UseBinaryFormatForTabletBootInfoBackup;
    NActors::TActorId TabletBootInfoBackup;

    const ui64 TenantHiveTabletId;

    const NMonitoring::TDynamicCounterPtr Counters;
    NMonitoring::TDynamicCounters::TCounterPtr HiveReconnectTimeCounter;
    ui64 HiveReconnectStartCycles = 0;
    bool HiveDisconnected = true;

public:
    explicit THiveProxyActor(THiveProxyConfig config);

    THiveProxyActor(
        THiveProxyConfig config,
        NMonitoring::TDynamicCounterPtr counters);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequest(
        const NActors::TActorContext& ctx,
        ui64 hive,
        NActors::IEventBase* request);

    ui64 GetHive(
        const NActors::TActorContext& ctx,
        ui64 tabletId,
        ui32 hiveIdx = Max());

    void SendNextCreateOrLookupRequest(
        const NActors::TActorContext& ctx,
        const TCreateOrLookupRequestQueue& queue);

    void SendLockReply(
        const NActors::TActorContext& ctx,
        TLockState* state,
        const NProto::TError& error = {});

    void SendUnlockReply(
        const NActors::TActorContext& ctx,
        TLockState* state,
        const NProto::TError& error = {});

    void SendLockLostNotification(
        const NActors::TActorContext& ctx,
        TLockState* state,
        const NProto::TError& error = {});

    void ScheduleSendTabletMetrics(const NActors::TActorContext& ctx, ui64 hive);

    void AddTabletMetrics(
        ui64 tabletId,
        const TTabletStats& tabletData,
        NKikimrHive::TEvTabletMetrics& record);

    void SendTabletMetrics(
        const NActors::TActorContext& ctx,
        ui64 hiveId,
        bool resend);

    void HandleConnect(
        NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDisconnect(
        NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleConnectionError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error,
        ui64 hive,
        bool connectFailed);

    void SendLockRequest(
        const NActors::TActorContext& ctx,
        ui64 hive,
        ui64 tabletId,
        bool reconnect = false);

    void HandleLockTabletExecutionResult(
        const NKikimr::TEvHive::TEvLockTabletExecutionResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLockTabletExecutionLost(
        const NKikimr::TEvHive::TEvLockTabletExecutionLost::TPtr& ev,
        const NActors::TActorContext& ctx);

    void SendUnlockRequest(
        const NActors::TActorContext& ctx,
        ui64 hive,
        ui64 tabletId);

    void HandleUnlockTabletExecutionResult(
        const NKikimr::TEvHive::TEvUnlockTabletExecutionResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void SendGetTabletStorageInfoRequest(
        const NActors::TActorContext& ctx,
        ui64 hive,
        ui64 tabletId);

    void HandleGetTabletStorageInfoRegistered(
        const NKikimr::TEvHive::TEvGetTabletStorageInfoRegistered::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetTabletStorageInfoResult(
        const NKikimr::TEvHive::TEvGetTabletStorageInfoResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRequestFinished(
        const TEvHiveProxyPrivate::TEvRequestFinished::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTabletMetrics(
        const NKikimr::TEvLocal::TEvTabletMetrics::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSendTabletMetrics(
        const TEvHiveProxyPrivate::TEvSendTabletMetrics::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleMetricsResponse(
        const NKikimr::TEvLocal::TEvTabletMetricsAck::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCreateTabletReply(
        const NKikimr::TEvHive::TEvCreateTabletReply::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTabletCreation(
        const NKikimr::TEvHive::TEvTabletCreationResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    STORAGE_HIVE_PROXY_REQUESTS(STORAGE_IMPLEMENT_REQUEST, TEvHiveProxy)
};

}   // namespace NCloud::NStorage
