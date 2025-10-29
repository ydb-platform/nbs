#include "kikimr_initializer.h"

#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/protos/config.pb.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TKikimrServicesInitializer::InitializeServices(
    TActorSystemSetup* setup,
    const TAppData* appData)
{
    //
    // GRPC proxy actor required for monitoring authorization
    //

    IActorPtr grpcReqProxy {
        NGRpcService::CreateGRpcRequestProxy(*Config, {})};
    setup->LocalServices.emplace_back(
        NGRpcService::CreateGRpcRequestProxyId(0),
        TActorSetupCmd(
            grpcReqProxy.release(),
            TMailboxType::ReadAsFilled,
            appData->UserPoolId));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NStorage
