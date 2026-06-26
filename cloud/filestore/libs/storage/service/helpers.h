#pragma once

#include "public.h"

#include <util/generic/fwd.h>

namespace NKikimrFileStore {
    class TConfig;
}

namespace NCloud::NFileStore::NProto {
    class TFileStore;
    class TFileStorePerformanceProfile;
}

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NKikimrFileStore::TConfig& config,
    NProto::TFileStore& fileStore);

class TInplaceBitmap
{
private:
    const TString& Data;

public:
    explicit TInplaceBitmap(const TString& data);
    bool Get(ui32 shardNo) const;
};

class TMutableInplaceBitmap
{
private:
    TString& Data;

public:
    explicit TMutableInplaceBitmap(TString& data);
    bool Get(ui32 shardNo) const;
    void Set(ui32 shardNo);
    void Clear();
};

}   // namespace NCloud::NFileStore::NStorage
