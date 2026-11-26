#pragma once

#include "peer.h"

#include <cloud/storage/core/libs/common/connection_handler.h>
#include <cloud/storage/core/libs/icv2/handshake/handshake.h>
#include <cloud/storage/core/libs/icv2/tcp/address_resolver.h>
#include <cloud/storage/core/libs/icv2/tcp/connector.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/generic/deque.h>
#include <util/generic/noncopyable.h>
#include <util/system/spinlock.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

class TPeerRegistry;

////////////////////////////////////////////////////////////////////////////////

struct TPeerDependencies
{
    IConnectorPtr Connector;
    IHandshakeProviderPtr Handshake;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    IAddressResolverPtr Resolver;
    ITaskQueuePtr Completions;
    IRequestChannelFactoryPtr ChannelFactory;
    TDuration ConnectTimeout;
};

////////////////////////////////////////////////////////////////////////////////

class TPeer final
    : public IPeer
    , public std::enable_shared_from_this<TPeer>
    , private TNonCopyable
{
    struct TActivity;
    struct TConnectionAttempt;
    struct TReadyChannel;
    struct TNotificationContext;
    enum class EPeerEvent;

    const TPeerDependencies Dependencies;
    const TString Host;
    const ui16 Port;

    TAdaptiveLock Lock;
    NProto::TError CloseError;
    std::shared_ptr<TActivity> Activity;
    TInstant RetryAt;
    ui64 Generation = 0;
    ui64 ConnectionFailures = 0;
    std::shared_ptr<TConnectionAttempt> Attempt;
    IRequestChannelPtr Channel;
    IClientConnectionHandlerPtr Handler;
    TDeque<EPeerEvent> Notifications;
    bool DispatchingNotifications = false;
    THotSwap<TReadyChannel> ReadyChannel;

public:
    TPeer(
        std::shared_ptr<TPeerRegistry> registry,
        ui64 id,
        TPeerDependencies dependencies,
        TString host,
        ui16 port,
        IClientConnectionHandlerPtr handler);

    ~TPeer() override;

    // IPeer implementation
    TRequestChannelFuture GetRequestChannel() override;
    TPeerStatus GetStatus() override;
    void Close() override;

    void Close(NProto::TError error);

private:
    IRequestChannelPtr GetReadyChannel() const;
    IRequestChannelPtr TakeChannel();
    TNotificationContext QueueNotification(EPeerEvent event);
    void DispatchNotifications(TNotificationContext context);
    void StartConnectionAttempt(ui64 generation);
    void ScheduleRetry(
        ui64 generation,
        TInstant deadline,
        std::weak_ptr<TActivity> activity);
    template <typename TResult>
    void HandleConnectionResult(
        std::shared_ptr<TConnectionAttempt> attempt,
        const NThreading::TFuture<TResult>& future);
    void ResolveHost(std::shared_ptr<TConnectionAttempt> attempt);
    void HandleConnectionResult(
        std::shared_ptr<TConnectionAttempt> attempt,
        const TResolveResult& result);
    void ConnectNext(
        std::shared_ptr<TConnectionAttempt> attempt,
        NProto::TError error);
    void ConnectNextAddress(
        std::shared_ptr<TConnectionAttempt> attempt,
        NProto::TError error);
    void HandleConnectionResult(
        std::shared_ptr<TConnectionAttempt> attempt,
        const TSocketResult& result);
    void HandleConnectionResult(
        std::shared_ptr<TConnectionAttempt> attempt,
        const TConnectHandshakeResult& result);
    void FinishAttempt(
        std::shared_ptr<TConnectionAttempt> attempt,
        TRequestChannelResult result);
    void OnClosed(ui64 generation);
    void DeliverAttemptResult(
        std::shared_ptr<TConnectionAttempt> attempt,
        TRequestChannelResult result,
        std::shared_ptr<TActivity> activity);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NIcV2
