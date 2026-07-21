#pragma once

#include "events.h"

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

NActors::IActor* CreateFinalizePublicationActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId,
    EFinalizePublicationOp op,
    const TString& userToken,
    const TString& callerSid);

} // namespace NKikimr::NPQ::NDeferredPublish
