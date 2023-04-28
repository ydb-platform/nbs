#pragma once

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TFreshBlob
{
    ui64 CommitId;
    TString Data;

    TFreshBlob(ui64 commitId, TString data)
        : CommitId(commitId)
        , Data(std::move(data))
    {}
};

}   // namespace NCloud::NBlockStore::NStorage
