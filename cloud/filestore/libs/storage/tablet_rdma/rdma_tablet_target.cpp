#include "rdma_tablet_target.h"

#include "rdma_server_handler.h"
#include "rdma_tablet_forwarder.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/rdma/server.h>
#include <cloud/filestore/libs/storage/api/components.h>

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/rdma/iface/config.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>
#include <cloud/storage/core/libs/rdma/impl/server.h>
#include <cloud/storage/core/libs/rdma/impl/verbs.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaTabletTargetConfig
{
    TString ListenAddress = "::";
    ui32 ListenPort = 10021;
    TDuration TabletRequestTimeout = TDuration::Minutes(1);
};

////////////////////////////////////////////////////////////////////////////////

class TRdmaTabletTarget final
    : public IStartable
{
private:
    const TRdmaTabletTargetConfig Config;
    const ILoggingServicePtr Logging;
    const IActorSystemPtr ActorSystem;
    const NCloud::NStorage::NRdma::IServerPtr Server;
    const TRdmaTabletRegistryPtr Registry;

public:
    TRdmaTabletTarget(
            TRdmaTabletTargetConfig config,
            ILoggingServicePtr logging,
            IActorSystemPtr actorSystem,
            NCloud::NStorage::NRdma::IServerPtr server,
            TRdmaTabletRegistryPtr registry)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , ActorSystem(std::move(actorSystem))
        , Server(std::move(server))
        , Registry(std::move(registry))
    {}

    void Start() override
    {
        const auto forwarderId = ActorSystem->Register(
            CreateRdmaTabletForwarderActor(Config.TabletRequestTimeout));
        Registry->AttachForwarder(forwarderId);

        auto handler = CreateRdmaTabletServerHandler(
            Logging,
            CreateActorSystemRdmaRequestHandler(
                ActorSystem,
                forwarderId,
                Registry));

        Server->Start();

        NCloud::NStorage::NRdma::IServerEndpointPtr endpoint;
        TString error;
        try {
            endpoint = Server->StartEndpoint(
                Config.ListenAddress,
                Config.ListenPort,
                handler);
        } catch (...) {
            error = CurrentExceptionMessage();
        }
        handler->SetEndpoint(endpoint);

        if (!endpoint) {
            TStringBuilder message;
            message << "unable to start RDMA endpoint " << Config.ListenAddress
                << ":" << Config.ListenPort;
            if (error) {
                message << ": " << error;
            }
            ReportTabletRdmaCritEvent(message);

            Server->Stop();
        }
    }

    void Stop() override
    {
        Server->Stop();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::NRdma::IServerPtr CreateRdmaServer(
    const NCloud::NProto::TRdmaServer& config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring)
{
    namespace NRdma = NCloud::NStorage::NRdma;

    try {
        return NRdma::CreateServer(
            NRdma::NVerbs::CreateVerbs(),
            NRdma::TObservabilityProvider(
                std::move(logging),
                std::move(monitoring),
                GetComponentName(TFileStoreComponents::RDMA),
                "filestore",
                "rdma_server"),
            NRdma::CreateServerConfigPtr(config));
    } catch (...) {
        ReportTabletRdmaCritEvent(
            TStringBuilder()
            << "unable to create RDMA server: " << CurrentExceptionMessage());
        return nullptr;
    }
}

IStartablePtr CreateRdmaTabletTarget(
    const NProto::TRdmaConfig& config,
    ILoggingServicePtr logging,
    IActorSystemPtr actorSystem,
    NCloud::NStorage::NRdma::IServerPtr server,
    TRdmaTabletRegistryPtr registry)
{
    TRdmaTabletTargetConfig targetConfig;
    if (config.GetListenAddress()) {
        targetConfig.ListenAddress = config.GetListenAddress();
    }
    if (config.GetListenPort()) {
        targetConfig.ListenPort = config.GetListenPort();
    }
    if (config.GetTabletRequestTimeout()) {
        targetConfig.TabletRequestTimeout =
            TDuration::MilliSeconds(config.GetTabletRequestTimeout());
    }

    return std::make_shared<TRdmaTabletTarget>(
        std::move(targetConfig),
        std::move(logging),
        std::move(actorSystem),
        std::move(server),
        std::move(registry));
}

}   // namespace NCloud::NFileStore::NStorage
