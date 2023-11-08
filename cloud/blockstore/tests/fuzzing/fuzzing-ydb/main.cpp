#include <cloud/blockstore/libs/daemon/ydb/bootstrap.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/tests/fuzzing/common/starter.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

#include <contrib/ydb/core/security/ticket_parser.h>

#include <library/cpp/testing/common/network.h>

////////////////////////////////////////////////////////////////////////////////

extern "C"
int LLVMFuzzerTestOneInput(const ui8* data, size_t size)
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

    NServer::TBootstrapYdb bootstrap(
        std::move(moduleFactories),
        std::move(serverModuleFactories),
        CreateDefaultDeviceHandlerFactory());

    TVector<NTesting::TPortHolder> ports;
    auto getFreePort = [&] () {
        return ToString(*ports.insert(ports.end(), NTesting::GetFreePort()));
    };

    TVector<TString> options {
        "blockstore-server",
        "--domain", "Root",
        "--node-broker", "localhost:9001",
        "--ic-port", getFreePort(),
        "--mon-port", getFreePort(),
        "--server-port", getFreePort(),
        "--data-server-port", getFreePort(),
        "--discovery-file", "nbs/nbs-discovery.txt",
        "--domains-file", "nbs/nbs-domains.txt",
        "--ic-file", "nbs/nbs-ic.txt",
        "--log-file", "nbs/nbs-log.txt",
        "--sys-file", "nbs/nbs-sys.txt",
        "--server-file", "nbs/nbs-server.txt",
        "--storage-file", "nbs/nbs-storage.txt",
        "--diag-file", "nbs/nbs-diag.txt",
        "--dr-proxy-file", "nbs/nbs-dr-proxy.txt",
        "--naming-file", "nbs/nbs-names.txt",
        "--diag-file", "nbs/nbs-diag.txt",
        "--auth-file", "nbs/nbs-auth.txt",
        "--dr-proxy-file", "nbs/nbs-dr-proxy.txt",
        "--local-storage-file", "nbs/nbs-local-storage.txt",
        "--service", "kikimr",
        "--load-configs-from-cms"
    };

    auto* starter = NFuzzing::TStarter::GetStarter(
        &bootstrap,
        std::move(options)
    );
    return starter ? starter->Run(data, size) : 0;
}
