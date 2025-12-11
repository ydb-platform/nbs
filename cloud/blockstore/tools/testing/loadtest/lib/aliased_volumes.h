#pragma once

#include "public.h"

#include <library/cpp/logger/log.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TAliasedVolume
{
    TString Name;
    TString Alias;
};

class TAliasedVolumes
{
private:
    TLog& Log;
    TVector<TAliasedVolume> AliasedVolumes;
    mutable TAdaptiveLock AliasedVolumesLock;

public:
    TAliasedVolumes(TLog& log)
        : Log(log)
    {}

public:
    void RegisterAlias(TString volumeName, TString alias);
    TString ResolveAlias(TString volumeName) const;
    bool IsAliased(const TString& diskId) const;
    void DestroyAliasedVolumesUnsafe(IClientFactory& clientFactory);
};

}   // namespace NCloud::NBlockStore::NLoadTest
