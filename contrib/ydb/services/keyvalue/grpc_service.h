#pragma once

#include <contrib/ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>

#include <contrib/ydb/library/grpc/server/grpc_server.h>
#include <contrib/ydb/library/actors/core/actorsystem_fwd.h>
#include <contrib/ydb/library/actors/core/actorid.h>


namespace NKikimr::NGRpcService {

class TKeyValueGRpcService
        : public NYdbGrpc::TGrpcServiceBase<Ydb::KeyValue::V1::KeyValueService>
{
public:
    TKeyValueGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
            NActors::TActorId grpcRequestProxyId);
    ~TKeyValueGRpcService();

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    NActors::TActorSystem* ActorSystem = nullptr;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    NActors::TActorId GRpcRequestProxyId;

    grpc::ServerCompletionQueue* CQ = nullptr;
};

} // namespace NKikimr::NGRpcService
