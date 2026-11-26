#pragma once

#include "peer_impl.h"
#include "peer_manager.h"

#include <util/generic/hash.h>
#include <util/generic/noncopyable.h>
#include <util/system/spinlock.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

class TPeerRegistry final: private TNonCopyable
{
    friend class TPeerManager;

    TAdaptiveLock Lock;
    THashMap<ui64, std::weak_ptr<TPeer>> Peers;
    enum class EState
    {
        Created,
        Running,
        StopRequested,
        Joining,
        Stopped,
    };

    EState State = EState::Created;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

public:
    void UnregisterPeer(ui64 id);
};

////////////////////////////////////////////////////////////////////////////////

class TPeerManager final
    : public IPeerManager
    , private TNonCopyable
{
    using EState = TPeerRegistry::EState;

    const TPeerDependencies Dependencies;
    const std::shared_ptr<TPeerRegistry> Registry;

    ui64 NextPeerId = 1;
    NThreading::TPromise<void> Joined = NThreading::NewPromise<void>();

public:
    explicit TPeerManager(TPeerDependencies dependencies);

    ~TPeerManager() override;

    // IPeerManager implementation
    void Start() override;

    TResultOrError<IPeerPtr> CreatePeer(
        TString host,
        ui16 port,
        IClientConnectionHandlerPtr handler) override;

    void RequestStop() override;

    void Stop() override;
};

}   // namespace NCloud::NIcV2
