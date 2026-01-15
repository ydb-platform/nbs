#include "request_checksum_helpers.h"

#include "block_checksum.h"

#include <cloud/blockstore/libs/common/constants.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const char* GetZeroBuffer()
{
    static constexpr char ZeroBuffer[MaxBlockSize]{};
    return ZeroBuffer;
}

template <typename TContainer>
NProto::TChecksum ConcatenateChecksums(const TContainer& checksums)
{
    Y_ABORT_UNLESS(!checksums.empty());
    ui64 byteCount = checksums[0].GetByteCount();
    TBlockChecksum checksumCalculator{checksums[0].GetChecksum()};

    for (size_t i = 1; i < static_cast<size_t>(checksums.size()); ++i) {
        checksumCalculator.Combine(
            checksums[i].GetChecksum(),
            checksums[i].GetByteCount());
        byteCount += checksums[i].GetByteCount();
    }

    NProto::TChecksum checksum;
    checksum.SetByteCount(byteCount);
    checksum.SetChecksum(checksumCalculator.GetValue());
    return checksum;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TChecksum CalculateChecksum(const TSgList& sglist)
{
    TBlockChecksum checksumCalculator;
    NProto::TChecksum checksum;
    for (auto blockData: sglist) {
        Y_DEBUG_ABORT_UNLESS(blockData.Size() > 0);
        if (!blockData.Data()) {
            checksumCalculator.Extend(GetZeroBuffer(), blockData.Size());
            checksum.SetByteCount(checksum.GetByteCount() + blockData.Size());
        } else {
            checksumCalculator.Extend(blockData.Data(), blockData.Size());
            checksum.SetByteCount(checksum.GetByteCount() + blockData.Size());
        }
    }
    checksum.SetChecksum(checksumCalculator.GetValue());
    return checksum;
}

NProto::TChecksum CalculateChecksum(
    const NProto::TIOVector& iov,
    ui32 blockSize)
{
    TBlockChecksum checksumCalculator;
    NProto::TChecksum checksum;
    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer.empty()) {
            checksumCalculator.Extend(GetZeroBuffer(), blockSize);
            checksum.SetByteCount(checksum.GetByteCount() + blockSize);
        } else {
            checksumCalculator.Extend(buffer.data(), buffer.size());
            checksum.SetByteCount(checksum.GetByteCount() + buffer.size());
        }
    }
    checksum.SetChecksum(checksumCalculator.GetValue());
    return checksum;
}

void CombineChecksumsInPlace(
    google::protobuf::RepeatedPtrField<NProto::TChecksum>& checksums)
{
    if (checksums.size() <= 1) {
        return;
    }

    NProto::TChecksum checksum = ConcatenateChecksums(checksums);
    checksums.Clear();
    checksums.Add(std::move(checksum));
}

[[nodiscard]] NProto::TChecksum CombineChecksums(
    const TVector<NProto::TChecksum>& checksums)
{
    NProto::TChecksum checksum;
    if (checksums.empty()) {
        return checksum;
    }

    return ConcatenateChecksums(checksums);
}

[[nodiscard]] NProto::TChecksum CombineChecksums(
    const google::protobuf::RepeatedPtrField<NProto::TChecksum>& checksums)
{
    NProto::TChecksum checksum;
    if (checksums.empty()) {
        return checksum;
    }

    return ConcatenateChecksums(checksums);
}

}   // namespace NCloud::NBlockStore
