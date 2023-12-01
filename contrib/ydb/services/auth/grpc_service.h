#pragma once

#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/grpc/server/grpc_server.h>
#include <contrib/ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <contrib/ydb/core/grpc_services/base/base_service.h>


namespace NKikimr {
namespace NGRpcService {

 class TGRpcAuthService : public TGrpcServiceBase<Ydb::Auth::V1::AuthService>
 {
 public:
     using TGrpcServiceBase<Ydb::Auth::V1::AuthService>::TGrpcServiceBase;

  private:
     void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
 };

} // namespace NGRpcService
} // namespace NKikimr
