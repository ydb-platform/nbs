#include <silk/util/init.h>

#include <silk/util/assert.h>
#include <silk/util/perf.h>
#include <silk/util/queue.h>
#include <silk/util/tsc.h>

// Suppress warnings emitted by librseq headers: volatile assignment in rseq_cs
// and unused parameters in the asm stubs.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-volatile"
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <rseq/rseq.h>
#pragma clang diagnostic pop

namespace silk
{

void initialize() noexcept
{
    // Arcadia's librseq neither auto-registers via a constructor nor relies
    // on glibc doing so (target platforms include glibc < 2.35). Register
    // rseq for the calling thread here so getCurrentProcessor's TLS read
    // returns a valid cpu_id; silk's own scheduler / worker threads do the
    // same in their own prologues.
    int r = rseq_register_current_thread();
    SILK_ASSERT(r == 0);

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
