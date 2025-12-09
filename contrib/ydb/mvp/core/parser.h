#pragma once
#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/event_local.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include "merger.h"

namespace NMVP {

NActors::TActorId CreateJsonParser(const NActors::TActorContext& ctx);

} // namespace NMVP
