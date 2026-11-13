#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Resolves host patterns into the hosts of a cell through EDS. One per
// process, shared by every cell, started and stopped by the cell manager.
// FetchHosts may be called concurrently, from several cells at once, and
// after Stop, when it answers with an error; the future may be completed on
// any thread. Callers poll at a fixed interval: it is the fetcher that
// spreads repeat requests out in time so that EDS is not hit in waves.
struct IEdsHostFetcher
    : public IStartable
{
    virtual NThreading::TFuture<TResultOrError<TVector<TString>>> FetchHosts(
        TVector<TString> hostPatterns) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Answers every pattern with the same hosts.
IEdsHostFetcherPtr CreateEdsHostFetcherStub(TVector<TString> hosts = {});

}   // namespace NCloud::NBlockStore::NCells
