#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

//
// Common.
//

NProto::TError ErrorNotSupported(const TString& message = {});
NProto::TError ErrorInvalidNodeType(ui64 nodeId);

//
// Nodes.
//

NProto::TError ErrorInvalidParent(ui64 nodeId);
NProto::TError ErrorInvalidTarget(ui64 nodeId, const TString& name = {});
NProto::TError ErrorAlreadyExists(const TString& path);
NProto::TError ErrorFailedToApplyCredentials(const TString& path);

//
// Directories.
//

NProto::TError ErrorIsDirectory(ui64 nodeId);
NProto::TError ErrorIsNotDirectory(ui64 nodeId);
NProto::TError ErrorIsNotEmpty(ui64 nodeId);

//
// Limits.
//

NProto::TError ErrorNameTooLong(const TString& name);
NProto::TError ErrorMaxLink(ui64 nodeId);
NProto::TError ErrorFileTooBig();
NProto::TError ErrorNoSpaceLeft();

//
// Arguments.
//

NProto::TError ErrorInvalidArgument();
NProto::TError ErrorInvalidHandle();
NProto::TError ErrorInvalidHandle(ui64 handle);

//
// XAttributes.
//

NProto::TError ErrorAttributeDoesNotExist(const TString& name);
NProto::TError ErrorAttributeAlreadyExists(const TString& name);
NProto::TError ErrorAttributeNameTooLong(const TString& name);
NProto::TError ErrorAttributeValueTooBig(const TString& name);
NProto::TError ErrorInvalidAttribute(const TString& name);

//
// Locks.
//

NProto::TError ErrorIncompatibleLocks();
NProto::TError ErrorIncompatibleLockType(int type);
NProto::TError ErrorIncompatibleLockWhence(int whence);
NProto::TError ErrorIncompatibleLockOriginLocks();
NProto::TError ErrorIncompatibleFileOpenMode();

//
// Session.
//

NProto::TError ErrorInvalidSession(
    const TString& clientId,
    const TString& sessionId,
    ui64 sessionSeqNo);
NProto::TError ErrorInvalidCheckpoint(const TString& checkpointId);
NProto::TError ErrorDuplicate();

////////////////////////////////////////////////////////////////////////////////

int FileStoreErrorToErrno(int error);
int ErrnoToFileStoreError(int error);

}   // namespace NCloud::NFileStore
