#pragma once

#include "public.h"

#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TRangeDefs = TVector<NKikimr::TTabletPercentileCounter::TRangeDef>;

const TRangeDefs& DefaultRangeDefs();

void IncrementFor(NKikimr::TTabletPercentileCounter& counter, ui64 value);

////////////////////////////////////////////////////////////////////////////////

struct TTxCountersDesc
{
    TVector<TString> Names;
    TVector<TRangeDefs> Ranges;
};

const TTxCountersDesc& TxSimpleCounters();
const TTxCountersDesc& TxCumulativeCounters();
const TTxCountersDesc& TxPercentileCounters();

////////////////////////////////////////////////////////////////////////////////

struct TTabletCountersDesc
{
    TVector<TString> Names;
    TVector<TRangeDefs> Ranges;

    TVector<const char*> NamePtrs;

    size_t Size = 0;
    size_t TxOffset = 0;
    size_t TxCountersSize = 0;
};

TTabletCountersDesc BuildTabletCounters(
    size_t counters,
    const char* const counterNames[],
    size_t txTypes,
    const char* const txTypeNames[],
    const TTxCountersDesc& txCounters);

std::unique_ptr<NKikimr::TTabletCountersBase> CreateTabletCounters(
    ui32 simpleCounters,
    ui32 cumulativeCounters,
    ui32 percentileCounters,
    const char* const simpleCounterNames[],
    const char* const cumulativeCounterNames[],
    const char* const percentileCounterNames[]);

std::unique_ptr<NKikimr::TTabletCountersWithTxTypes> CreateTabletCountersWithTxTypes(
    const TTabletCountersDesc& simpleCounters,
    const TTabletCountersDesc& cumulativeCounters,
    const TTabletCountersDesc& percentileCounters);

}   // namespace NCloud::NFileStore::NStorage
