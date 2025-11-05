#pragma once
#include "abstract.h"
#include <contrib/ydb/library/actors/core/event_local.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/conclusion/result.h>
#include <contrib/ydb/core/base/events.h>

namespace NKikimr::NConveyor {

struct TEvExecution {
    enum EEv {
        EvNewTask = EventSpaceBegin(TKikimrEvents::ES_CONVEYOR),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONVEYOR), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        YDB_READONLY_DEF(ITask::TPtr, Task);
    public:
        TEvNewTask() = default;

        explicit TEvNewTask(ITask::TPtr task);
    };
};

}
