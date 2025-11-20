#include "events.h"
#include <contrib/ydb/library/actors/core/log.h>

namespace NKikimr::NConveyor {

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task)
    : Task(task)
{
    AFL_VERIFY(Task);
}

}
