#pragma once

#include <cloud/filestore/tools/ops/write_back_cache_state_tool/protos/write_back_cache_state_tool.pb.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

////////////////////////////////////////////////////////////////////////////////

struct IStateFileLocator
{
    virtual ~IStateFileLocator() = default;

    virtual TResultOrError<NProto::TStateFileList> ListStateFiles() = 0;

    virtual TResultOrError<TString> LocateStateFile(
        const TString& fsId,
        const TString& sessionId,
        NProto::EStateFileType fileType) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IStateFileLocator> CreateStateFileLocator(
    const TString& stateDir);

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
