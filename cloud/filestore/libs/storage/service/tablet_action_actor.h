#pragma once

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/public.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
class TTabletActionActor final
    : public NActors::TActorBootstrapped<TTabletActionActor<TRequest, TResponse>>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    using TBase =
        NActors::TActorBootstrapped<TTabletActionActor<TRequest, TResponse>>;

public:
    TTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        const TResponse::ProtoRecordType& responseRecord);

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleResponse(
        const TResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
TTabletActionActor<TRequest, TResponse>::TTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

template <typename TRequest, typename TResponse>
void TTabletActionActor<TRequest, TResponse>::Bootstrap(
    const NActors::TActorContext& ctx)
{
    typename TRequest::ProtoRecordType request;
    if (!google::protobuf::util::JsonStringToMessage(Input, &request).ok()) {
        ReplyAndDie(
            ctx,
            TErrorResponse(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!request.GetFileSystemId()) {
        ReplyAndDie(
            ctx,
            TErrorResponse(E_ARGUMENT, "FileSystem id should be supplied"));
        return;
    }

    auto requestToTablet = std::make_unique<TRequest>();
    requestToTablet->Record = std::move(request);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    TBase::Become(&TTabletActionActor<TRequest, TResponse>::StateWork);
}

template <typename TRequest, typename TResponse>
void TTabletActionActor<TRequest, TResponse>::ReplyAndDie(
    const NActors::TActorContext& ctx,
    const TResponse::ProtoRecordType& response)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
        response.GetError());

    google::protobuf::util::MessageToJsonString(
        response,
        msg->Record.MutableOutput());

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
void TTabletActionActor<TRequest, TResponse>::HandleResponse(
    const TResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->Record);
}

}   // namespace NCloud::NFileStore::NStorage
