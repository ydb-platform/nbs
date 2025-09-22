#include "disk_agent_actor.h"
#include "library/cpp/protobuf/json/proto2json.h"

#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/storage/disk_common/monitoring_utils.h>

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TProcessActionActor: public TActorBootstrapped<TProcessActionActor>
{
private:
    const NActors::TActorId OwnerId;
    const bool IsOpen;
    const TString DeviceName;
    const ui64 DeviceGeneration;

    TRequestInfoPtr RequestInfo;

public:
    TProcessActionActor(
            NActors::TActorId ownerId,
            bool isOpen,
            TString deviceName,
            ui64 deviceGeneration,
            TRequestInfoPtr requestInfo)
        : OwnerId(ownerId)
        , IsOpen(isOpen)
        , DeviceName(std::move(deviceName))
        , DeviceGeneration(deviceGeneration)
        , RequestInfo(std::move(requestInfo))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        IEventBasePtr request;

        NProto::TPathToGeneration pathToGen;
        pathToGen.SetDiskPath(DeviceName);
        pathToGen.SetGeneration(DeviceGeneration);
        if (IsOpen) {
            auto attachRequest =
                std::make_unique<TEvDiskAgent::TEvAttachPathRequest>();

            *attachRequest->Record.AddDisksToAttach() = std::move(pathToGen);
            request = std::move(attachRequest);
        } else {
            auto detachRequest =
                std::make_unique<TEvDiskAgent::TEvDetachPathRequest>();
            *detachRequest->Record.AddDisksToDetach() = std::move(pathToGen);
            request = std::move(detachRequest);
        }

        NCloud::Send(ctx, OwnerId, std::move(request));
        Become(&TThis::StateWork);
    }

private:
    template <typename TEvent>
    void HandleResult(const TEvent& ev, const TActorContext& ctx)
    {
        auto record = ev->Get()->Record;

        TStringStream out;

        NProtobufJson::Proto2Json(record, out);

        NCloud::Reply(
            ctx,
            *RequestInfo,
            std::make_unique<NMon::TEvHttpInfoRes>(out.Str()));
        Die(ctx);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvDiskAgent::TEvAttachPathResponse, HandleResult);
            HFunc(TEvDiskAgent::TEvDetachPathResponse, HandleResult);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_AGENT_WORKER,
                    __PRETTY_FUNCTION__);
                break;
        }
    }
};

bool ProcessHttpActon(
    const NActors::NMon::TEvHttpInfo::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto& request = ev->Get()->Request;

    auto methodType = request.GetMethod();
    auto params = request.GetPostParams();
    const auto& action = params.Get("action");

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    static auto gen = 1;

    if (action == "closeDevice" && methodType == HTTP_METHOD_POST) {
        NCloud::Register<TProcessActionActor>(
            ctx,
            ctx.SelfID,
            false,
            params.Get("deviceName"),
            ++gen,
            std::move(requestInfo));
        return true;
    }
    if (action == "openDevice" && methodType == HTTP_METHOD_POST) {
        NCloud::Register<TProcessActionActor>(
            ctx,
            ctx.SelfID,
            true,
            params.Get("deviceName"),
            ++gen,
            std::move(requestInfo));
        return true;
    }

    return false;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleHttpInfo(
    const NMon::TEvHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& request = ev->Get()->Request;

    TString uri { request.GetUri() };
    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_AGENT,
        "HTTP request: %s", uri.c_str());

    TStringStream out;

    if (ProcessHttpActon(ev, ctx)) {
        return;
    }

    HTML(out) {
        if (CurrentStateFunc() == &TThis::StateIdle) {
            DIV() { out << "Unregistered (Idle)"; }
        } else {
            switch (RegistrationState) {
                case ERegistrationState::NotStarted:
                    DIV() { out << "Initialization in progress"; }
                    break;
                case ERegistrationState::InProgress:
                    DIV() { out << "Registration in progress"; }
                    break;
                case ERegistrationState::Registered:
                    DIV() { out << "Registered"; }
                    break;
            }
        }

        TAG(TH3) { out << "Devices"; }
        RenderDevices(out);

        TAG(TH3) { out << "Config"; }
        AgentConfig->DumpHtml(out);

        if (RdmaServer) {
            TAG(TH3) { out << "RdmaServer"; }
            RdmaServer->DumpHtml(out);
        }
    }

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<NMon::TEvHttpInfoRes>(out.Str()));
}

void TDiskAgentActor::RenderDevices(IOutputStream& out) const
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "UUID"; }
                    TABLEH() { out << "S/N"; }
                    TABLEH() { out << "Name"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "State Timestamp"; }
                    TABLEH() { out << "State Message"; }
                    TABLEH() { out << "Block size"; }
                    TABLEH() { out << "Blocks"; }
                    TABLEH() { out << "Pool"; }
                    TABLEH() { out << "Transport id"; }
                    TABLEH() { out << "Rdma endpoint"; }
                    TABLEH() { out << "Writer session"; }
                    TABLEH() { out << "Reader sessions"; }
                    TABLEH() { out << "Path generation"; }
                    TABLEH () {
                        out << "Open close device";
                    }
                }
            }

            for (const auto& config: State->GetDevices()) {
                const auto& uuid = config.GetDeviceUUID();

                TABLER() {
                    TABLED() { out << uuid; }
                    TABLED() { out << config.GetSerialNumber(); }
                    TABLED() { out << config.GetDeviceName(); }
                    TABLED() {
                        DumpDeviceState(
                            out,
                            config.GetState(),
                            State->GetDeviceStateFlags(uuid));
                    }
                    TABLED() {
                        if (config.GetStateTs()) {
                            out << TInstant::MicroSeconds(config.GetStateTs());
                        }
                    }
                    TABLED() {
                        out << config.GetStateMessage();
                    }
                    TABLED() { out << config.GetBlockSize(); }
                    TABLED() {
                        const auto bytes = config.GetBlockSize() * config.GetBlocksCount();
                        out << config.GetBlocksCount() << " (" << FormatByteSize(bytes) << ")";
                    }
                    TABLED() { out << config.GetPoolName(); }
                    TABLED() { out << config.GetTransportId(); }
                    TABLED() {
                        if (config.HasRdmaEndpoint()) {
                            const auto& e = config.GetRdmaEndpoint();
                            out << e.GetHost() << ":" << e.GetPort();
                        }
                    }
                    TABLED() {
                        auto [id, ts, seqNo] = State->GetWriterSession(uuid);
                        if (id) {
                            TABLE_SORTABLE_CLASS("table table-bordered") {
                            TABLER() {
                                TABLED() { out << id; }
                                TABLED() { out << ts; }
                                TABLED() { out << seqNo; }
                            }}
                        }
                    }
                    TABLED() {
                        if (auto sessions = State->GetReaderSessions(uuid)) {
                            TABLE_SORTABLE_CLASS("table table-bordered") {
                                for (const auto& [id, ts, seqNo]: sessions) {
                                    TABLER() {
                                        TABLED() { out << id; }
                                        TABLED() { out << ts; }
                                        TABLED() { out << seqNo; }
                                    }
                                }
                            }
                        }
                    }

                    TABLED () {
                        out << State->DeviceGeneration(uuid);
                    }

                    if (State->IsDeviceAttached(uuid)) {
                        TABLED () {
                            out << Sprintf(
                                R"(<form name="openDevice" method="POST">
                                <input type='hidden' name='action' value='openDevice'/>
                                <input type='hidden' name='deviceName' value='%s'/>
                                <button type="submit">Open device</button>
                            </form>)",
                                config.GetDeviceName().c_str());
                        }
                    } else {
                        TABLED () {
                            out << Sprintf(
                                R"(<form name="closeDevice" method="POST">
                                <input type='hidden' name='action' value='closeDevice'/>
                                <input type='hidden' name='deviceName' value='%s'/>
                                <button type="submit">Close device</button>
                            </form>)",
                                config.GetDeviceName().c_str());
                        }
                    }
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
