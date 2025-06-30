#include <cloud/blockstore/config/root_kms.pb.h>
#include <cloud/blockstore/libs/daemon/ydb/bootstrap.h>
#include <cloud/blockstore/libs/kms/iface/compute_client.h>
#include <cloud/blockstore/libs/kms/iface/kms_client.h>
#include <cloud/blockstore/libs/kms/impl/compute_client.h>
#include <cloud/blockstore/libs/kms/impl/kms_client.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/libs/opentelemetry/impl/trace_service_client.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/server.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/root_kms/iface/client.h>
#include <cloud/blockstore/libs/root_kms/impl/client.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>

#include <cloud/storage/core/libs/daemon/app.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/config.h>

#include <contrib/ydb/core/driver_lib/run/factories.h>
#include <contrib/ydb/core/security/ticket_parser.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    auto moduleFactories = std::make_shared<NKikimr::TModuleFactories>();
    moduleFactories->CreateTicketParser = NKikimr::CreateTicketParser;

    auto serverModuleFactories =
        std::make_shared<NServer::TServerModuleFactories>();
    serverModuleFactories->LogbrokerServiceFactory = [] (
        NLogbroker::TLogbrokerConfigPtr config,
        NCloud::ILoggingServicePtr logging)
    {
        Y_UNUSED(config);
        return NLogbroker::CreateServiceNull(logging);
    };

    serverModuleFactories->IamClientFactory = [] (
        NCloud::NIamClient::TIamClientConfigPtr config,
        NCloud::ILoggingServicePtr logging,
        NCloud::ISchedulerPtr scheduler,
        NCloud::ITimerPtr timer)
    {
        Y_UNUSED(config);
        Y_UNUSED(logging);
        Y_UNUSED(scheduler);
        Y_UNUSED(timer);
        return NCloud::NIamClient::CreateIamTokenClientStub();
    };

    serverModuleFactories->ComputeClientFactory = [] (
        NProto::TGrpcClientConfig config,
        NCloud::ILoggingServicePtr logging)
    {
        if (config.GetAddress()) {
            return NCloud::NBlockStore::CreateComputeClient(
                std::move(logging),
                std::move(config));
        }

        return NCloud::NBlockStore::CreateComputeClientStub();
    };

    serverModuleFactories->KmsClientFactory = [] (
        NProto::TGrpcClientConfig config,
        NCloud::ILoggingServicePtr logging)
    {
        if (config.GetAddress()) {
            return NCloud::NBlockStore::CreateKmsClient(
                std::move(logging),
                std::move(config));
        }

        return NCloud::NBlockStore::CreateKmsClientStub();
    };

    serverModuleFactories->RootKmsClientFactory = [] (
        const NProto::TRootKmsConfig& config,
        NCloud::ILoggingServicePtr logging)
    {
        if (config.GetAddress()) {
            return NCloud::NBlockStore::CreateRootKmsClient(
                std::move(logging),
                {.Address = config.GetAddress(),
                 .RootCertsFile = config.GetRootCertsFile(),
                 .CertChainFile = config.GetCertChainFile(),
                 .PrivateKeyFile = config.GetPrivateKeyFile()});
        }

        return NCloud::NBlockStore::CreateRootKmsClientStub();
    };

    serverModuleFactories->TraceServiceClientFactory = [] (
        NProto::TGrpcClientConfig config,
        NCloud::ILoggingServicePtr logging)
    {
        if (config.GetAddress()) {
            return NCloud::NBlockStore::CreateTraceServiceClient(
                std::move(logging),
                std::move(config));
        }

        return NCloud::NBlockStore::CreateTraceServiceClientStub();
    };

    serverModuleFactories->SpdkFactory = [] (
        NSpdk::TSpdkEnvConfigPtr config)
    {
        Y_UNUSED(config);
        return NServer::TSpdkParts {
            .Env = NSpdk::CreateEnvStub(),
            .VhostCallbacks = {},
            .LogInitializer = {},
        };
    };

    serverModuleFactories->RdmaClientFactory = [] (
        NCloud::ILoggingServicePtr logging,
        NCloud::IMonitoringServicePtr monitoring,
        NRdma::TClientConfigPtr config)
    {
        return NRdma::CreateClient(
            NRdma::NVerbs::CreateVerbs(),
            std::move(logging),
            std::move(monitoring),
            std::move(config));
    };

    serverModuleFactories->RdmaServerFactory = [] (
        NCloud::ILoggingServicePtr logging,
        NCloud::IMonitoringServicePtr monitoring,
        NRdma::TServerConfigPtr config)
    {
        return NRdma::CreateServer(
            NRdma::NVerbs::CreateVerbs(),
            std::move(logging),
            std::move(monitoring),
            std::move(config));
    };

    NServer::TBootstrapYdb bootstrap(
        std::move(moduleFactories),
        std::move(serverModuleFactories),
        CreateDefaultDeviceHandlerFactory());
    return NCloud::DoMain(bootstrap, argc, argv);
}
