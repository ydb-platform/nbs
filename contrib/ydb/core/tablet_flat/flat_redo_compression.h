#pragma once

#include "defs.h"
#include <contrib/ydb/core/base/events.h>
#include <contrib/ydb/library/actors/core/event_local.h>

namespace NKikimr::NTabletFlatExecutor {

struct TEvRedoEncoded : TEventLocal<TEvRedoEncoded,
    EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR) + 1152>
{
    TEvRedoEncoded(ui32 step, TString body)
        : Step(step)
        , Body(std::move(body))
    {}

    const ui32 Step;
    TString Body;
};

// Register on a separate mailbox: compression must not block the tablet mailbox.
IActor* CreateRedoCompressionActor(TActorId owner, ui32 step, TString redo);

}
