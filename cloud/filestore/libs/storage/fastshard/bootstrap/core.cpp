#include "core.h"

#include <silk/fibers/fiber.h>
#include <silk/util/init.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

void Init()
{
    silk::initialize();
    silk::FiberScheduler::initialize();
}

void Destroy()
{
    silk::FiberScheduler::destroy();
    silk::destroy();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
