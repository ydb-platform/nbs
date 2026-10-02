/*******************************************************************************

Read-only Blockstore configuration monitoring. The Static column shows
local values saved before CMS; Dynamic shows private_database_config.
ConfigsManager owns the renderer and updates its monitoring metadata.
Configuration inputs must remain valid during RenderHtml().

*******************************************************************************/

#pragma once

#include <cloud/blockstore/config/blockstore.pb.h>
#include <cloud/blockstore/libs/config/blockstore_config.h>

#include <cloud/storage/core/libs/config/runtime_config.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <optional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Accepted configuration origin and whether a runtime delivery changed its
// values.
enum class EConfigUpdateStatus
{
    Startup,
    Runtime,
    RuntimeUnchanged,
};

// Monitoring metadata owned by the renderer and updated by ConfigsManager.
struct TBlockstoreConfigRendererData
{
    // Manager startup time or receipt time of the last accepted runtime update.
    // Zero until ConfigsManager initializes the renderer during Bootstrap().
    TInstant LastUpdateTime;

    // Accepted source origin: startup, runtime or runtime (unchanged).
    EConfigUpdateStatus UpdateStatus = EConfigUpdateStatus::Startup;

    // Last rejected delivery reason; owned and empty after an accepted
    // delivery.
    TString RejectedUpdateReason;

    // Presence of private_database_config, including an empty section.
    bool DynamicConfigPresent = false;

    // Initial dispatcher delivery identifier; empty before the first request.
    // Repeated delivery retains this cookie; a new update receives another one.
    std::optional<ui64> StartupDeliveryCookie;

    // Rejected paths owned for the displayed publication; empty at startup.
    NConfig::TRuntimeConfigDiagnostics RuntimeDiagnostics;
};

// HTML renderer for local config saved before CMS, private YAML, ICB overrides
// and current effective values. Default-construct it in ConfigsManager, update
// Data on config delivery, and borrow configuration inputs only during
// rendering.
class TBlockstoreConfigRenderer
{
public:
    // Monitoring metadata owned by this renderer; updated only by its actor.
    TBlockstoreConfigRendererData Data;

    // Render the configuration sources, ICB overrides, and effective values as
    // an HTML page with collapsible top-level sections.
    TString RenderHtml(
        const IBlockstoreConfig& config,
        const NProto::TBlockstoreConfig& staticConfig,
        const NProto::TBlockstoreConfig& dynamicConfig,
        const NStorage::TStorageConfigControls& controls) const;
};

}   // namespace NCloud::NBlockStore
