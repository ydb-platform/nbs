#pragma once

#include "public.h"

#include <contrib/ydb/core/base/logoblob.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TScanDiskSettings
{
    ui32 BatchSize = 0;
    bool CheckIntegrity = false;

    ui32 MaxInFlight = 0;
    TDuration CheckTimeout;
    ui32 MaxRetries = 0;
    TDuration RetryTimeout;
    ui32 MaxReportedBlobs = 0;
    ui32 MaxDataInfoSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

enum class EScanDiskBlobVerdict
{
    Broken,
    Degraded,
    Unchecked,
};

struct TScanDiskBlobResult
{
    NKikimr::TLogoBlobID BlobId;
    ui32 BSGroupId = 0;
    EScanDiskBlobVerdict Verdict = EScanDiskBlobVerdict::Broken;
    TString Status;
    TString PlacementStatus;
    TString DataStatus;
    TString ErrorReason;
    TString DataInfo;
};

struct TScanDiskResults
{
    TVector<NKikimr::TLogoBlobID> BrokenBlobs;

    ui64 DeadBlobs = 0;
    ui64 CheckedBlobs = 0;
    ui64 ReplicatingBlobs = 0;
    ui64 DegradedBlobs = 0;
    ui64 UncheckedBlobs = 0;

    TVector<TScanDiskBlobResult> BlobResults;
    ui64 DroppedBlobResults = 0;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
