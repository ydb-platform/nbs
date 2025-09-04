#include "grpc_service.h"

#include <contrib/ydb/core/grpc_services/grpc_helper.h>
#include <contrib/ydb/core/grpc_services/base/base.h>
#include <contrib/ydb/core/grpc_services/service_config.h>
#include <contrib/ydb/core/jaeger_tracing/request_discriminator.h>
#include "contrib/ydb/library/grpc/server/grpc_method_setup.h"

namespace NKikimr::NGRpcService {

TConfigGRpcService::TConfigGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId) \
    : ActorSystem(actorSystem) \
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TConfigGRpcService::~TConfigGRpcService() = default;

void TConfigGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TConfigGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

    #define SETUP_BS_METHOD(methodName, method, rlMode, requestType, auditModeFlags) \
        SETUP_METHOD(methodName, method, rlMode, requestType, Config, config, auditModeFlags)

    SETUP_BS_METHOD(ReplaceConfig, DoReplaceConfig, Rps, CONFIG_REPLACECONFIG, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_BS_METHOD(FetchConfig, DoFetchConfig, Rps, CONFIG_FETCHCONFIG, TAuditMode::NonModifying());
    SETUP_BS_METHOD(BootstrapCluster, DoBootstrapCluster, Rps, CONFIG_BOOTSTRAP, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

    #undef SETUP_BS_METHOD
}

} // namespace NKikimr::NGRpcService
