#include "service_actions.h"

#include "service.h"
#include "service_method.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TActionHandler> LowerCaseNames(
    THashMap<TString, TActionHandler> actions)
{
    THashMap<TString, TActionHandler> result;
    for (auto& [name, handler]: actions) {
        TString action = name;
        action.to_lower();
        result.emplace(std::move(action), std::move(handler));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TActionsService final
    : public TBlockStoreImpl<TActionsService, IBlockStore>
{
private:
    const IBlockStorePtr Service;
    const THashMap<TString, TActionHandler> Actions;

public:
    TActionsService(
            IBlockStorePtr service,
            THashMap<TString, TActionHandler> actions)
        : Service(std::move(service))
        , Actions(LowerCaseNames(std::move(actions)))
    {}

    void Start() override
    {
        Service->Start();
    }

    void Stop() override
    {
        Service->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr ctx,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        return TMethod::Execute(
            Service.get(),
            std::move(ctx),
            std::move(request));
    }

    TFuture<NProto::TExecuteActionResponse> ExecuteAction(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TExecuteActionRequest> request) override
    {
        TString action = request->GetAction();
        action.to_lower();

        const auto* handler = Actions.FindPtr(action);
        if (!handler) {
            return Service->ExecuteAction(std::move(ctx), std::move(request));
        }
        return (*handler)(request->GetInput());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateActionsService(
    IBlockStorePtr service,
    THashMap<TString, TActionHandler> actions)
{
    return std::make_shared<TActionsService>(
        std::move(service),
        std::move(actions));
}

}   // namespace NCloud::NBlockStore
