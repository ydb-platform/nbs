#pragma once

#include "public.h"

#include "part_nonrepl_migration_common_actor.h"

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// This actor is used to restore lagging agent data and make it consistent with
// other replicas. It does this by copying dirty ranges using a bitmap.
class TLaggingAgentMigrationActor final
    : public TNonreplicatedPartitionMigrationCommonActor
    , public IMigrationOwner
{
private:
    const TStorageConfigPtr Config;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const NActors::TActorId ParentActorId;
    const NActors::TActorId TargetActorId;
    const NActors::TActorId SourceActorId;
    const TString AgentId;

public:
    TLaggingAgentMigrationActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TNonreplicatedPartitionConfigPtr partConfig,
        NActors::TActorId parentActorId,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString rwClientId,
        NActors::TActorId targetActorId,
        NActors::TActorId sourceActorId,
        TCompressedBitmap migrationBlockMap,
        TString agentId);

    ~TLaggingAgentMigrationActor() override;

private:
    // IMigrationOwner implementation
    void OnBootstrap(const NActors::TActorContext& ctx) override;
    bool OnMessage(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev) override;
    void OnMigrationProgress(
        const NActors::TActorContext& ctx,
        ui64 migrationIndex) override;
    void OnMigrationFinished(const NActors::TActorContext& ctx) override;
    void OnMigrationError(const NActors::TActorContext& ctx) override;

private:
    void HandleStartLaggingAgentMigration(
        const TEvNonreplPartitionPrivate::TEvStartLaggingAgentMigration::TPtr&
            ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
