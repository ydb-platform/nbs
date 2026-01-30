#pragma once

#include <cloud/storage/core/libs/ss_proxy/protos/path_description_backup.pb.h>
#include <cloud/storage/core/protos/error.pb.h>

namespace NCloud::NStorage::NSSProxy {

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TError SavePathDescriptionBackupToChunkedBinaryFormat(
    const TString& filePath,
    const NProto::TPathDescriptionBackup& pathDescriptionBackup);

NCloud::NProto::TError LoadPathDescriptionBackupFromChunkedBinaryFormat(
    const TString& filePath,
    NProto::TPathDescriptionBackup* pathDescriptionBackup);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NStorage::NSSProxy
