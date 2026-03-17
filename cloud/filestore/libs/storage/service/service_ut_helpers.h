#pragma once

#include <cloud/filestore/libs/diagnostics/profile_log.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NFileStore::NStorage {

class TTestProfileLog
    : public IProfileLog
{
public:
    TMap<ui32, TVector<TRecord>> Requests;

    void Start() override;

    void Stop() override;

    void Write(TRecord record) override;
};

TString GenerateValidateData(ui32 size, ui32 seed = 0);

}   // namespace NCloud::NFileStore::NStorage
