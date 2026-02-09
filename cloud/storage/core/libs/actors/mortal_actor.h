#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud {

class IMortalActor
{
public:
    virtual void Poison(const NActors::TActorContext& ctx) = 0;

    virtual ~IMortalActor() = default;
};

}   // namespace NCloud
