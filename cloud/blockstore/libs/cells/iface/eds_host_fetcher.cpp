#include "eds_host_fetcher.h"

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEdsHostFetcherStub final
    : public IEdsHostFetcher
{
private:
    const TVector<TString> Hosts;

public:
    explicit TEdsHostFetcherStub(TVector<TString> hosts)
        : Hosts(std::move(hosts))
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TFuture<TResultOrError<TVector<TString>>> FetchHosts(
        TVector<TString> hostPatterns) override
    {
        Y_UNUSED(hostPatterns);
        return MakeFuture(TResultOrError<TVector<TString>>(Hosts));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEdsHostFetcherPtr CreateEdsHostFetcherStub(TVector<TString> hosts)
{
    return std::make_shared<TEdsHostFetcherStub>(std::move(hosts));
}

}   // namespace NCloud::NBlockStore::NCells
