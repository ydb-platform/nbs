#pragma once

#include <cloud/blockstore/libs/daemon/common/bootstrap.h>

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/thread/factory.h>

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

class TBufferedClient;

}   // namespace NVHost

namespace NCloud::NBlockStore::NFuzzing {

////////////////////////////////////////////////////////////////////////////////

class TStarter: public IStartable
{
private:
    NServer::TBootstrapBase& Bootstrap;
    std::unique_ptr<NVHost::TBufferedClient> Client;

    ILoggingServicePtr FuzzerLogging;
    TLog Log;

    THolder<IThreadFactory::IThread> Thread;

public:
    static TStarter* GetStarter(
        NServer::TBootstrapBase* bootstrap,
        TVector<TString> options);

    void ParseOptions(TVector<TString> options);

    void Start() override;
    void Stop() override;

    TLog& GetLogger();
    int Run(const ui8* data, size_t size);

private:
    TStarter(NServer::TBootstrapBase& bootstrap);
};

}   // namespace NCloud::NBlockStore::NFuzzing
