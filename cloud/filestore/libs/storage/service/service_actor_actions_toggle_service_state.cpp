#include "service_actor.h"

#include <cloud/filestore/private/api/protos/actions.pb.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;

using namespace NKikimr;

void TStorageServiceActor::PerformToggleServiceStateAction(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    TString input)
{
    NProtoPrivate::TToggleServiceStateRequest request;
    if (!google::protobuf::util::JsonStringToMessage(input, &request).ok()) {
        auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
            TErrorResponse(E_ARGUMENT, "Failed to parse input"));
        NCloud::Reply(ctx, *requestInfo, std::move(msg));
        return;
    }

    ServiceState = request.GetDesiredServiceState();
    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "Service state changed to %s",
        EServiceState_Name(ServiceState).Quote().c_str());

    NProtoPrivate::TToggleServiceStateResponse response;

    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>();
    google::protobuf::util::MessageToJsonString(
        response,
        msg->Record.MutableOutput());
    NCloud::Reply(ctx, *requestInfo, std::move(msg));
}

}   // namespace NCloud::NFileStore::NStorage
