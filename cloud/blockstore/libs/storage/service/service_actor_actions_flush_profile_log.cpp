#include "service_actor.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFlushProfileLogActor final
    : public TActorBootstrapped<TFlushProfileLogActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    IProfileLogPtr ProfileLog;

public:
    TFlushProfileLogActor(
        TRequestInfoPtr requestInfo,
        IProfileLogPtr profileLog);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TFlushProfileLogActor::TFlushProfileLogActor(
        TRequestInfoPtr requestInfo,
        IProfileLogPtr profileLog)
    : RequestInfo(std::move(requestInfo))
    , ProfileLog(std::move(profileLog))
{}

void TFlushProfileLogActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);
    const bool success = ProfileLog->Flush();

    NProto::TError error;
    if (!success) {
        error.SetCode(E_REJECTED);
        error.SetMessage("Couldn't flush profile log.");
    }
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TFlushProfileLogActor::StateWork)
{
    HandleUnexpectedEvent(
        ev,
        TBlockStoreComponents::SERVICE,
        __PRETTY_FUNCTION__);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateFlushProfileLogActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    Y_UNUSED(input);
    return {std::make_unique<TFlushProfileLogActor>(
        std::move(requestInfo),
        ProfileLog)};
}

}   // namespace NCloud::NBlockStore::NStorage
