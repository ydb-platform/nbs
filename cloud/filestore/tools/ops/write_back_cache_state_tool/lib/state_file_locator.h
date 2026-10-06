#pragma once

#include <cloud/filestore/tools/ops/write_back_cache_state_tool/protos/write_back_cache_state_tool.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/system/file.h>

#include <memory>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

////////////////////////////////////////////////////////////////////////////////

struct IStateFileLocator
{
    virtual ~IStateFileLocator() = default;

    virtual TResultOrError<NProto::TStateFileList> ListStateFiles() = 0;

    // An empty fileType matches state files of any type.
    virtual TResultOrError<TFile> LocateAndOpenStateFile(
        const TString& fsId,
        const TString& sessionId,
        TMaybe<NProto::EStateFileType> fileType,
        bool readOnly) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IStateFileLocator> CreateStateFileLocator(
    const TString& stateDir);

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
