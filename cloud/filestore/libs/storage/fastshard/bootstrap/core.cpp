#include "core.h"

#include <silk/fibers/fiber.h>
#include <silk/util/init.h>
#include <silk/util/logger.h>

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

void EnableDebugLogging()
{
    silk::Logger::setLevel(silk::LogLevel::DEBUG);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
