#include <cloud/blockstore/libs/cells/iface/forward_service.h>

#include "inbound_activity.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

// The whole bypass rests on one assumption: the trusted server-stamped
// sources below are reachable only from the underlay. The CellId header is a
// plain client header and carries no trust on its own; it only marks a
// request as an inter-cell forward. Trust comes from the source, which the
// server stamps by listening port and a client cannot forge. If that
// perimeter ever loosens, skipping authorization here becomes a direct
// bypass - see the design's Security section.

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
    // effect (see the design's decision 5)
    return source == NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL;
}

////////////////////////////////////////////////////////////////////////////////

class TInboundMonPage final: public THtmlMonPage
{
private:
    const std::shared_ptr<TCellInboundActivity> Activity;
    const ITimerPtr Timer;

public:
    TInboundMonPage(
            std::shared_ptr<TCellInboundActivity> activity,
            ITimerPtr timer)
        : THtmlMonPage("Inbound", "Inbound", true)
        , Activity(std::move(activity))
        , Timer(std::move(timer))
    {}

    void OutputContent(IMonHttpRequest& request) override
    {
        auto& out = request.Output();
        auto rows = Activity->Snapshot(Timer->Now());

        HTML(out) {
            TAG(TH3) { out << "Inbound inter-cell connections"; }
            TABLE_SORTABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "CellId"; }
                        TABLEH() { out << "Peer"; }
                        TABLEH() { out << "DiskId"; }
                        TABLEH() { out << "ClientId"; }
                        TABLEH() { out << "Mounts"; }
                        TABLEH() { out << "Unmounts"; }
                        TABLEH() { out << "Describes"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& row: rows) {
                        TABLER() {
                            TABLED() { out << row.CellId; }
                            TABLED() { out << row.Peer; }
                            TABLED() { out << row.DiskId; }
                            TABLED() { out << row.ClientId; }
                            TABLED() { out << row.Mounts; }
                            TABLED() { out << row.Unmounts; }
                            TABLED() { out << row.Describes; }
                        }
                    }
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCellForwardService final
    : public TBlockStoreImpl<TCellForwardService, IBlockStore>
{
private:
    const IBlockStorePtr Authorized;
    const IBlockStorePtr Trusted;
    const ITimerPtr Timer;
    const std::shared_ptr<TCellInboundActivity> Activity;
    TLog Log;

public:
    TCellForwardService(
            IBlockStorePtr authorized,
            IBlockStorePtr trusted,
            IMonitoringServicePtr monitoring,
            ILoggingServicePtr logging,
            ITimerPtr timer)
        : Authorized(std::move(authorized))
        , Trusted(std::move(trusted))
        , Timer(std::move(timer))
        , Activity(std::make_shared<TCellInboundActivity>())
    {
        Log = logging->CreateLog("BLOCKSTORE_CELLS");

        if (monitoring) {
            auto rootPage =
                monitoring->RegisterIndexPage("blockstore", "BlockStore");
            auto* cellsPage = static_cast<TIndexMonPage&>(*rootPage)
                                  .RegisterIndexPage("Cells", "Cells");
            cellsPage->Register(new TInboundMonPage(Activity, Timer));
        }
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
            // the inter-cell marker on a channel we do not trust: a
            // misconfiguration or a forgery attempt. It is authorized like
            // any other request - the gate below is false - but it is worth
            // shouting about
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

            Activity->Record(
                cellId,
                peer,
                diskId,
                headers.GetClientId(),
                TMethod::BlockStoreRequest,
                Timer->Now());

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
    IMonitoringServicePtr monitoring,
    ILoggingServicePtr logging,
    ITimerPtr timer)
{
    return std::make_shared<TCellForwardService>(
        std::move(authorized),
        std::move(trusted),
        std::move(monitoring),
        std::move(logging),
        std::move(timer));
}

}   // namespace NCloud::NBlockStore::NCells
