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

void Init(const cpu_set_t& cpuMask)
{
    silk::FiberScheduler::Options options;
    options.cpuMask = cpuMask;

    silk::initialize();
    silk::FiberScheduler::initialize(&options);
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
