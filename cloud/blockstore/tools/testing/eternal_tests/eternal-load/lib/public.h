#pragma once

#include <util/datetime/base.h>
#include <util/system/defaults.h>
#include <util/stream/file.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TBlockData
{
    ui64 RequestNumber = 0;
    ui64 PartNumber = 0;
    ui64 BlockIndex = 0;
    ui64 RangeIdx = 0;
    ui64 RequestTimestamp = 0;
    ui64 TestTimestamp = 0;
    ui64 TestId = 0;
    ui64 Checksum = 0;

    bool operator<(const TBlockData& r) const
    {
        return memcmp(this, &r, sizeof(TBlockData)) < 0;
    }
};

inline IOutputStream& operator<<(IOutputStream& out, const TBlockData& data)
{
    out << "{"
        << " RequestNumber " << data.RequestNumber
        << " PartNumber " << data.PartNumber
        << " BlockIndex " << data.BlockIndex
        << " RangeIdx " << data.RangeIdx
        << " RequestTimestamp " << TInstant::MicroSeconds(data.RequestTimestamp)
        << " TestTimestamp " << TInstant::MicroSeconds(data.TestTimestamp)
        << " TestId " << data.TestId
        << " Checksum " << data.Checksum
        << "}";

    return out;
}

////////////////////////////////////////////////////////////////////////////////

struct IRequestGenerator;
using IRequestGeneratorPtr = std::shared_ptr<IRequestGenerator>;

struct IConfigHolder;
using IConfigHolderPtr = std::shared_ptr<IConfigHolder>;

}   // namespace NCloud::NBlockStore
