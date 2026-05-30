#include "service_actor.h"

#include <cloud/filestore/private/api/protos/actions.pb.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::PerformConfigureUnconfirmedWriteForgetAction(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    TString input)
{
    NProtoPrivate::TConfigureUnconfirmedWriteForgetRequest request;
    if (!google::protobuf::util::JsonStringToMessage(input, &request).ok()) {
        auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
            TErrorResponse(E_ARGUMENT, "Failed to parse input"));
        NCloud::Reply(ctx, *requestInfo, std::move(msg));
        return;
    }

    UnconfirmedWriteRequestsToForget = request.GetRequestsToForget();

    LOG_WARN(
        ctx,
        TFileStoreComponents::SERVICE,
        "Configured unconfirmed write forget hook: requestsToForget=%lu",
        UnconfirmedWriteRequestsToForget);

    NProtoPrivate::TConfigureUnconfirmedWriteForgetResponse response;
    response.SetRequestsToForget(UnconfirmedWriteRequestsToForget);

    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>();
    google::protobuf::util::MessageToJsonString(
        response,
        msg->Record.MutableOutput());
    NCloud::Reply(ctx, *requestInfo, std::move(msg));
}

}   // namespace NCloud::NFileStore::NStorage
