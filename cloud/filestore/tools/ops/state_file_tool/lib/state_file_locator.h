#pragma once

#include <cloud/filestore/tools/ops/state_file_tool/protos/state_file_tool.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/system/file.h>

#include <memory>

namespace NCloud::NFileStore::NStateFileTool {

////////////////////////////////////////////////////////////////////////////////

struct IStateFileLocator
{
    virtual ~IStateFileLocator() = default;

    virtual TResultOrError<NProto::TStateFileList> ListStateFiles() = 0;

    // An empty sessionId matches all sessions. An empty fileType matches all
    // file types. Paths outside the filters are not inspected.
    // The returned file is open but unlocked. The caller must acquire any lock
    // needed while accessing it.
    virtual TResultOrError<TFile> LocateAndOpenStateFile(
        const TString& fsId,
        const TString& sessionId,
        TMaybe<NProto::EStateFileType> fileType,
        bool readOnly) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IStateFileLocator> CreateStateFileLocator(
    const TString& stateDir);

}   // namespace NCloud::NFileStore::NStateFileTool
