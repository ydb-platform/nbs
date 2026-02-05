#pragma once

#include "public.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IFsCountersProvider
{
    virtual ~IFsCountersProvider() = default;

    // Creates filesystem/client/cloud/folder hierarchy, returns the
    // folder-level counters. If already registered, increments ref count and
    // returns existing counters.
    virtual NMonitoring::TDynamicCountersPtr Register(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& cloudId,
        const TString& folderId) = 0;

    // Decrements ref count. Removes the entire client subtree when ref count
    // reaches 0.
    virtual void Unregister(
        const TString& fileSystemId,
        const TString& clientId) = 0;

    // TODO(#5150)
    // Updates cloud/folder in the counter hierarchy, moving existing counters
    // Should be removed in #5150
    virtual void UpdateCloudAndFolder(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& newCloudId,
        const TString& newFolderId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFsCountersProviderPtr CreateFsCountersProvider(
    TString component,
    NMonitoring::TDynamicCountersPtr rootCounters);

IFsCountersProviderPtr CreateFsCountersProviderStub();

}   // namespace NCloud::NFileStore
