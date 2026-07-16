#include <silk/util/init.h>

#include <silk/util/assert.h>
#include <silk/util/perf.h>
#include <silk/util/queue.h>
#include <silk/util/tsc.h>

#include <silk/util/platform.h>

namespace silk
{

void initialize() noexcept
{
    // Arcadia's librseq neither auto-registers via a constructor nor relies
    // on glibc doing so (target platforms include glibc < 2.35). Register
    // rseq for the calling thread here so getCurrentProcessor's TLS read
    // returns a valid cpu_id. Every other thread that later calls into
    // silk hits the same guard lazily through getCurrentProcessor().
    ensureRseqRegistered();

    Tsc::initialize();
    Perf::initialize();
    QueueBase::initialize();
}

void destroy() noexcept
{
    QueueBase::destroy();
    Perf::destroy();
}

} // namespace silk
