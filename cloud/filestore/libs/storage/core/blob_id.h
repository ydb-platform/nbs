#pragma once

#include "public.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/core/base/logoblob.h>

namespace NCloud::NFileStore::NProtoPrivate {

////////////////////////////////////////////////////////////////////////////////

//
// Converters between NKikimr::TLogoBlobID and its local proto copy
// NProtoPrivate::TFileStoreBlobID. Defined in the proto namespace so that
// unqualified calls resolve via argument-dependent lookup, mirroring the
// NKikimr helpers for NKikimrProto.TLogoBlobID.
//

/**
 * Parses a blob id from its proto representation.
 *
 * @param proto - Proto representation of the blob id.
 * @return - The parsed blob id.
 */
NKikimr::TLogoBlobID LogoBlobIDFromLogoBlobID(const TFileStoreBlobID& proto);

/**
 * Serializes a blob id into its proto representation.
 *
 * @param id - Blob id to serialize.
 * @param proto - (out) Proto representation of the blob id.
 */
void LogoBlobIDFromLogoBlobID(
    const NKikimr::TLogoBlobID& id,
    TFileStoreBlobID* proto);

}   // namespace NCloud::NFileStore::NProtoPrivate

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

//
// Merge the blob id converters for both proto representations into one
// overload set so that unqualified calls work for either of them.
//

using NKikimr::LogoBlobIDFromLogoBlobID;
using NProtoPrivate::LogoBlobIDFromLogoBlobID;

/**
 * Copies a blob id between its two proto representations.
 *
 * @param blobId - Local proto representation of the blob id.
 * @param to - (out) ydb proto representation of the blob id.
 */
void Convert(
    const NProtoPrivate::TFileStoreBlobID& blobId,
    NKikimrProto::TLogoBlobID& to);

}   // namespace NCloud::NFileStore::NStorage
