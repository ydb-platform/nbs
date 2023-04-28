#include "ping_metrics.h"

namespace {

////////////////////////////////////////////////////////////////////////////////

void Update(
    TInstant now,
    TDuration halfDecay,
    ui64 value,
    TInstant lastTs,
    double& counter)
{
    const double passed = (now - lastTs).MicroSeconds();
    counter = counter * pow(2, -passed / halfDecay.MicroSeconds()) + value;
}

}   // namespace

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TPingMetrics::Update(TInstant now, TDuration halfDecay, ui64 bytes)
{
    if (LastUpdateTs) {
        ::Update(now, halfDecay, bytes ? 1 : 0, LastUpdateTs, Requests);
        ::Update(now, halfDecay, bytes, LastUpdateTs, Bytes);
    } else {
        Requests = 1;
        Bytes = bytes;
    }

    LastUpdateTs = now;
}

}   // namespace NCloud::NBlockStore::NStorage
