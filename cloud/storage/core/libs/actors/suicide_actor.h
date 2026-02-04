#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud {

class ISuicideActor
{
public:
    virtual void Suicide(const NActors::TActorContext& ctx) = 0;

    virtual ~ISuicideActor() = default;
};

}   // namespace NCloud
