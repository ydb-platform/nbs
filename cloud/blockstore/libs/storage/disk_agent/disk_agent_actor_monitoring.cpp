#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/storage/disk_common/monitoring_utils.h>
#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

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
                            State->IsDeviceDisabled(uuid)
                                ? EDeviceStateFlags::DISABLED
                                : EDeviceStateFlags::NONE);
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
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
