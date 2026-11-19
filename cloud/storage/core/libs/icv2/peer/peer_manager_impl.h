#pragma once

#include "peer_impl.h"
#include "peer_manager.h"

#include <util/generic/hash.h>
#include <util/system/mutex.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

class TPeerManager final
    : public IPeerManager
    , public std::enable_shared_from_this<TPeerManager>
{
    const TPeerDependencies Dependencies;

    TMutex Mutex;
    THashMap<ui64, std::weak_ptr<TPeer>> Peers;
    ui64 NextPeerId = 1;
    enum class EState
    {
        Created,
        Running,
        StopRequested,
        Joining,
        Stopped,
    };

    EState State = EState::Created;
    NThreading::TPromise<void> Joined = NThreading::NewPromise<void>();
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

public:
    explicit TPeerManager(TPeerDependencies dependencies);

    ~TPeerManager() override;

    // IPeerManager implementation
    void Start() override;

    TResultOrError<IPeerPtr> CreatePeer(
        TString host,
        ui16 port,
        IClientEndpointHandlerPtr handler) override;

    void RequestStop() override;

    void Stop() override;

    void UnregisterPeer(ui64 id);
};

}   // namespace NCloud::NIcV2
