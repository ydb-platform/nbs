#include "test_conductor.h"

#include <library/cpp/neh/rpc.h>

#include <util/generic/hash_set.h>
#include <util/string/printf.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct TFakeConductor::TImpl
{
    ui16 Port;
    NNeh::IServicesRef Loop;

    TVector<THostInfo> HostInfo;
    THashSet<TString> DroppedGroups;
    TVector<std::shared_ptr<NNeh::IRequest>> DroppedRequests;
    TMutex Lock;

    TImpl(ui16 port)
        : Port(port)
    {}

    ~TImpl()
    {
        if (Loop) {
            with_lock (Lock) {
                for (auto& droppedRequest: DroppedRequests) {
                    droppedRequest->SendError(
                        NNeh::IRequest::ServiceUnavailable);
                }

                DroppedRequests.clear();
            }

            Loop->SyncStopFork();
        }
    }

    void ServeRequest(const NNeh::IRequestRef& req)
    {
        NNeh::TDataSaver ds;

        TStringBuf apiUrl, group;
        req->Service().RSplit('/', apiUrl, group);

        if (group == "broken_group") {
            req->SendError(NNeh::IRequest::ServiceUnavailable);
        } else {
            with_lock (Lock) {
                if (DroppedGroups.contains(group)) {
                    DroppedRequests.emplace_back(req.Release());

                    return;
                }

                for (const auto& hostInfo: HostInfo) {
                    if (hostInfo.Group == group) {
                        ds << hostInfo.Host << Endl;
                    }
                }
            }

            req->SendReply(ds);
        }
    }

    void PreStart()
    {
        Loop = NNeh::CreateLoop();
        Loop->Add(Sprintf("http://*:%u/*", Port), *this);
    }

    void ForkStart()
    {
        PreStart();
        Loop->ForkLoop(1);
    }

    void Start()
    {
        PreStart();
        Loop->Loop(1);
    }
};

TFakeConductor::TFakeConductor(ui16 port)
    : Impl(new TImpl(port))
{}

TFakeConductor::~TFakeConductor()
{}

void TFakeConductor::ForkStart()
{
    Impl->ForkStart();
}

void TFakeConductor::Start()
{
    Impl->Start();
}

void TFakeConductor::SetHostInfo(TVector<THostInfo> hostInfo)
{
    with_lock (Impl->Lock) {
        Impl->HostInfo = std::move(hostInfo);
    }
}

void TFakeConductor::SetDrop(TString group, bool drop)
{
    with_lock (Impl->Lock) {
        if (drop) {
            Impl->DroppedGroups.emplace(std::move(group));
        } else {
            Impl->DroppedGroups.erase(group);
        }
    }
}

}   // namespace NCloud::NBlockStore::NDiscovery
