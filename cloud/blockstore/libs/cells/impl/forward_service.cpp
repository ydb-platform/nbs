#include <cloud/blockstore/libs/cells/iface/forward_service.h>

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsWhitelisted(EBlockStoreRequest request)
{
    return request == EBlockStoreRequest::MountVolume
        || request == EBlockStoreRequest::UnmountVolume
        || request == EBlockStoreRequest::DescribeVolume;
}

bool IsTrustedSource(NCloud::NProto::ERequestSource source)
{
    // the single trusted source for now; rdma adds its own here when control
    // starts flowing over it - a deliberate security decision, never a side
    // effect
    return source == NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL;
}

////////////////////////////////////////////////////////////////////////////////

class TCellForwardService final
    : public TBlockStoreImpl<TCellForwardService, IBlockStore>
{
private:
    const IBlockStorePtr Authorized;
    const IBlockStorePtr Trusted;
    const std::shared_ptr<TCellInboundActivity> Activity;
    const ITimerPtr Timer;
    TLog Log;

public:
    TCellForwardService(
            IBlockStorePtr authorized,
            IBlockStorePtr trusted,
            std::shared_ptr<TCellInboundActivity> activity,
            ILoggingServicePtr logging,
            ITimerPtr timer)
        : Authorized(std::move(authorized))
        , Trusted(std::move(trusted))
        , Activity(std::move(activity))
        , Timer(std::move(timer))
    {
        Log = logging->CreateLog("BLOCKSTORE_CELLS");
    }

    void Start() override
    {
        // Authorized wraps the same inner stack Trusted points at, so
        // starting it starts everything once; starting Trusted too would
        // double-start the shared inner services
        Authorized->Start();
    }

    void Stop() override
    {
        Authorized->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Authorized->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        const auto& headers = request->GetHeaders();
        const bool whitelisted = IsWhitelisted(TMethod::BlockStoreRequest);
        const bool hasCellId = !headers.GetCellId().empty();
        const bool trustedSource =
            IsTrustedSource(headers.GetInternal().GetRequestSource());

        if (whitelisted && hasCellId && !trustedSource) {
            STORAGE_WARN(
                "[cell " << headers.GetCellId() << "] inter-cell marker from"
                    << " an untrusted source "
                    << static_cast<int>(
                           headers.GetInternal().GetRequestSource())
                    << ", authorizing normally");
        }

        if (whitelisted && hasCellId && trustedSource) {
            const auto& cellId = headers.GetCellId();
            const auto& peer = headers.GetInternal().GetPeer();
            const auto diskId = GetDiskId(*request);

            if (Activity) {
                Activity->Record(
                    peer,
                    diskId,
                    headers.GetClientId(),
                    TMethod::BlockStoreRequest,
                    Timer->Now());
            }

            // audited because authorization is skipped here: a record of who
            // went past it, after the fact
            STORAGE_INFO(
                "[cell " << cellId << "] inter-cell "
                    << GetBlockStoreRequestName(TMethod::BlockStoreRequest)
                    << " without authorization, peer=" << peer
                    << " disk=" << diskId);

            return TMethod::Execute(
                Trusted.get(),
                std::move(callContext),
                std::move(request));
        }

        return TMethod::Execute(
            Authorized.get(),
            std::move(callContext),
            std::move(request));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateCellForwardService(
    IBlockStorePtr authorized,
    IBlockStorePtr trusted,
    std::shared_ptr<TCellInboundActivity> activity,
    ILoggingServicePtr logging,
    ITimerPtr timer)
{
    return std::make_shared<TCellForwardService>(
        std::move(authorized),
        std::move(trusted),
        std::move(activity),
        std::move(logging),
        std::move(timer));
}

}   // namespace NCloud::NBlockStore::NCells
