#pragma once

#include <cloud/blockstore/public/api/protos/io.pb.h>

#include <cloud/storage/core/libs/common/sglist.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

NProto::TChecksum CalculateChecksum(const TSgList& sglist);
NProto::TChecksum CalculateChecksum(
    const NProto::TIOVector& iov,
    ui32 blockSize);

void CombineChecksumsInPlace(
    google::protobuf::RepeatedPtrField<NProto::TChecksum>& checksums);
[[nodiscard]] NProto::TChecksum CombineChecksums(
    const TVector<NProto::TChecksum>& checksums);

}   // namespace NCloud::NBlockStore
