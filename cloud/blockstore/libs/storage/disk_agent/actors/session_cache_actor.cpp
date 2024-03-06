#include "session_cache_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/components.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/system/fs.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSessionCacheActor
    : public TActorBootstrapped<TSessionCacheActor>
{
private:
    const TString CachePath;

    TVector<NProto::TDiskAgentDeviceSession> Sessions;
    TRequestInfoPtr RequestInfo;
    NActors::IEventBasePtr Response;

public:
    TSessionCacheActor(
            TVector<NProto::TDiskAgentDeviceSession> sessions,
            TString cachePath,
            TRequestInfoPtr requestInfo,
            NActors::IEventBasePtr response)
        : CachePath {std::move(cachePath)}
        , Sessions {std::move(sessions)}
        , RequestInfo {std::move(requestInfo)}
        , Response {std::move(response)}
    {
        ActivityType = TBlockStoreComponents::DISK_AGENT_WORKER;
    }

    void Bootstrap(const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

void TSessionCacheActor::Bootstrap(const TActorContext& ctx)
{
    try {
        NProto::TDiskAgentDeviceSessionCache proto;
        proto.MutableSessions()->Assign(
            std::make_move_iterator(Sessions.begin()),
            std::make_move_iterator(Sessions.end())
        );

        const TString tmpPath {CachePath + ".tmp"};

        SerializeToTextFormat(proto, tmpPath);

        if (!NFs::Rename(tmpPath, CachePath)) {
            char buf[64] = {};

            const auto ec = errno;
            ythrow TServiceError {MAKE_SYSTEM_ERROR(ec)}
                << strerror_r(ec, buf, sizeof(buf));
        }
    } catch (...) {
        LOG_ERROR_S(
            ctx,
            ActivityType,
            "Can't update session cache: " << CurrentExceptionMessage());

        ReportDiskAgentSessionCacheUpdateError();
    }

    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::move(Response));

    Die(ctx);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IActor> CreateSessionCacheActor(
    TVector<NProto::TDiskAgentDeviceSession> sessions,
    TString cachePath,
    TRequestInfoPtr requestInfo,
    NActors::IEventBasePtr response)
{
    return std::make_unique<TSessionCacheActor>(
        std::move(sessions),
        std::move(cachePath),
        std::move(requestInfo),
        std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
