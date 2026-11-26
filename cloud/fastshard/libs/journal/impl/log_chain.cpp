#include "log_chain.h"

#include <util/generic/utility.h>
#include <util/string/builder.h>

#include <algorithm>
#include <iterator>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Returns the first record with Lsn above |lsn|, or the end of |chain| when
// there is no such record. The records in |chain| are sorted by Lsn.
TDeque<TLogRecordPtr>::const_iterator FindFirstAbove(
    const TDeque<TLogRecordPtr>& chain,
    ui64 lsn)
{
    return std::upper_bound(
        chain.begin(),
        chain.end(),
        lsn,
        [](ui64 lsn, const TLogRecordPtr& record)
        { return lsn < record->Lsn; });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TLogRecordChain::InitLastChainedLsn(ui64 lsn)
{
    LsnLowWatermark = lsn;

    std::lock_guard lock(PoolLock);
    LastChainedLsn = lsn;
}

TResultOrError<bool> TLogRecordChain::Insert(TLogRecordPtr record)
{
    if (record->PrevLsn >= record->Lsn) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "record with lsn " << record->Lsn
                             << " has prev lsn " << record->PrevLsn
                             << ", not below its lsn");
    }

    std::lock_guard lock(PoolLock);

    //
    // Check if the record is the same as the last chained record. If yes,
    // it is a retry: it gets the same answer as the first write
    //

    if (record->Lsn == LastChainedLsn) {
        if (!LastChainedRecord) {
            record->Promise.SetValue(MakeError(S_ALREADY));
            return false;
        }

        LastChainedRecord->Promise.GetFuture().Subscribe(
            [record = std::move(record)](const auto& future)
            { record->Promise.SetValue(future.GetValue()); });
        return false;
    }

    if (record->Lsn < LastChainedLsn) {
        std::lock_guard chainLock(ChainLock);

        //
        // Find the chained record with the same lsn. This is rare: a retry
        // of an old record. The lsn is above zero here, so "- 1" is safe
        //

        auto it = FindFirstAbove(RecordChain, record->Lsn - 1);
        if (it == RecordChain.end() || (*it)->Lsn != record->Lsn) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "record with lsn " << record->Lsn
                    << " is below the last chained lsn " << LastChainedLsn);
        }

        (*it)->Promise.GetFuture().Subscribe(
            [record = std::move(record)](const auto& future)
            { record->Promise.SetValue(future.GetValue()); });
        return false;
    }

    //
    // Check if the same record is already in the pool. If yes, it is a
    // retry: it gets the same answer as the first write
    //

    if (auto it = RecordPool.find(record->PrevLsn); it != RecordPool.end()) {
        const auto& held = it->second.Record;
        if (held->Lsn == record->Lsn) {
            held->Promise.GetFuture().Subscribe(
                [record = std::move(record)](const auto& future)
                { record->Promise.SetValue(future.GetValue()); });
            return false;
        }
        // another record already continues from the same lsn
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "record with lsn " << record->Lsn << " has prev lsn "
                << record->PrevLsn << ", but the record with lsn "
                << held->Lsn << " already has this prev lsn");
    }

    // TODO(#6956): check, that there is no record with the same Lsn

    // the boundaries of the chained run are all held, so a record
    // starting below LastChainedLsn starts inside another record
    if (record->PrevLsn < LastChainedLsn) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "record with lsn " << record->Lsn << " has prev lsn "
                << record->PrevLsn << ", below the last chained lsn "
                << LastChainedLsn);
    }

    RecordPool.emplace(record->PrevLsn, TEntry{.Record = record});
    return true;
}

bool TLogRecordChain::MarkAsReady(ui64 prevLsn)
{
    std::lock_guard lock(PoolLock);

    auto it = RecordPool.find(prevLsn);
    if (it == RecordPool.end()) {
        return false;
    }

    it->second.Ready = true;
    return true;
}

bool TLogRecordChain::Remove(ui64 prevLsn)
{
    std::lock_guard lock(PoolLock);

    auto it = RecordPool.find(prevLsn);
    if (it == RecordPool.end()) {
        return false;
    }

    if (it->second.Ready) {
        return false;
    }

    RecordPool.erase(it);
    return true;
}

TVector<TLogRecordPtr> TLogRecordChain::RemoveStrandedRecords()
{
    std::lock_guard lock(PoolLock);

    TVector<TLogRecordPtr> erased;
    // ready records with prevLsn below LastChainedLsn can never join
    for (auto it = RecordPool.begin(); it != RecordPool.end();) {
        if (it->first < LastChainedLsn && it->second.Ready) {
            erased.push_back(std::move(it->second.Record));
            RecordPool.erase(it++);
        } else {
            ++it;
        }
    }

    return erased;
}

TLogRecordPtr TLogRecordChain::ChainNextRecord(ui64 lsn)
{
    TLogRecordPtr record;

    //
    // Take the ready record that continues the chain out of the pool
    //

    {
        std::lock_guard lock(PoolLock);
        if (LastChainedLsn != lsn) {
            return nullptr;
        }

        auto it = RecordPool.find(lsn);
        if (it == RecordPool.end() || !it->second.Ready) {
            return nullptr;
        }

        record = std::move(it->second.Record);
        RecordPool.erase(it);
        LastChainedRecord = record;
        LastChainedLsn = record->Lsn;
    }

    //
    // Add it to the chain after the pool lock is released: the write path
    // never holds both locks
    //

    {
        std::lock_guard lock(ChainLock);
        RecordChain.push_back(record);
    }

    return record;
}

TVector<TLogRecordPtr> TLogRecordChain::ExtractRecords(ui64 lsnLowWatermark)
{
    std::lock_guard lock(ChainLock);

    LsnLowWatermark = lsnLowWatermark;

    //
    // Move the records up to the watermark out in one go: the write path
    // waits for this lock
    //

    const size_t recordCount =
        FindFirstAbove(RecordChain, lsnLowWatermark) - RecordChain.cbegin();

    auto first = RecordChain.begin();
    auto last = first + recordCount;

    TVector<TLogRecordPtr> result(
        std::make_move_iterator(first),
        std::make_move_iterator(last));

    RecordChain.erase(first, last);

    return result;
}

ui64 TLogRecordChain::GetLsnLowWatermark() const
{
    return LsnLowWatermark;
}

std::pair<TVector<TLogRecordPtr>, ui64> TLogRecordChain::GetTail(
    ui64 afterLsn,
    ui32 maxRecordCount) const
{
    std::lock_guard lock(ChainLock);

    auto first = FindFirstAbove(RecordChain, afterLsn);

    size_t recordCount = RecordChain.end() - first;
    if (maxRecordCount > 0) {
        recordCount = Min<size_t>(recordCount, maxRecordCount);
    }

    TVector<TLogRecordPtr> records(first, first + recordCount);
    return std::make_pair(std::move(records), LsnLowWatermark);
}

}   // namespace NCloud::NJournalled
