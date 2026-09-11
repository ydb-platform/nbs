#include "log_chain.h"

#include <util/generic/utility.h>
#include <util/string/builder.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

void TLogRecordChain::InitLastErasedLsn(ui64 lsn)
{
    with_lock (Lock) {
        LastErasedLsn = lsn;
        LastChainedLsn = lsn;
    }
}

TResultOrError<TLogRecordPtr> TLogRecordChain::Insert(TLogRecordPtr record)
{
    if (record->PrevLsn >= record->Lsn) {
        return MakeError(E_ARGUMENT);
    }

    with_lock (Lock) {
        if (record->Lsn <= LastErasedLsn) {
            return MakeError(E_INVALID_STATE);
        }

        if (auto it = Records.find(record->PrevLsn); it != Records.end()) {
            const auto& held = it->second.Record;
            if (held->Lsn == record->Lsn) {
                return held;
            }
            // another record already continues from the same lsn
            return MakeError(E_INVALID_STATE);
        }

        // the boundaries of the chained run are all held, so a record
        // starting below LastChainedLsn starts inside another record
        if (record->PrevLsn < LastChainedLsn) {
            return MakeError(E_INVALID_STATE);
        }

        Records.emplace(record->PrevLsn, TEntry{.Record = record});
    }

    return record;
}

bool TLogRecordChain::MarkAsReady(ui64 prevLsn)
{
    with_lock (Lock) {
        auto it = Records.find(prevLsn);
        if (it == Records.end()) {
            return false;
        }

        it->second.Ready = true;

        for (;;) {
            auto next = Records.find(LastChainedLsn);
            if (next == Records.end() || !next->second.Ready) {
                break;
            }
            LastChainedLsn = next->second.Record->Lsn;
        }
    }

    return true;
}

bool TLogRecordChain::Remove(ui64 prevLsn)
{
    with_lock (Lock) {
        auto it = Records.find(prevLsn);
        if (it == Records.end()) {
            return false;
        }

        if (it->second.Ready) {
            return false;
        }

        Records.erase(it);
    }

    return true;
}

TResultOrError<TVector<TLogRecordPtr>> TLogRecordChain::EraseUpTo(ui64 lsn)
{
    TVector<TLogRecordPtr> records;

    with_lock (Lock) {
        if (lsn > LastChainedLsn) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "lsn " << lsn
                                 << " reaches past the chained run ending at "
                                 << LastChainedLsn);
        }

        for (;;) {
            auto it = Records.find(LastErasedLsn);
            if (it == Records.end() || it->second.Record->Lsn > lsn) {
                break;
            }

            records.push_back(std::move(it->second.Record));
            Records.erase(it);
            LastErasedLsn = records.back()->Lsn;
        }

        // ready records chaining from below the watermark can never join
        for (auto it = Records.begin(); it != Records.end();) {
            if (it->first < LastErasedLsn && it->second.Ready) {
                records.push_back(std::move(it->second.Record));
                Records.erase(it++);
            } else {
                ++it;
            }
        }
    }

    return records;
}

TLogRecordPtr TLogRecordChain::GetOldest() const
{
    with_lock (Lock) {
        return GetNextImpl(LastErasedLsn);
    }
}

TLogRecordPtr TLogRecordChain::GetNext(ui64 lsn) const
{
    with_lock (Lock) {
        return GetNextImpl(lsn);
    }
}

TLogRecordPtr TLogRecordChain::GetNextImpl(ui64 lsn) const
{
    auto it = Records.find(lsn);
    if (it == Records.end() || !it->second.Ready) {
        return nullptr;
    }
    return it->second.Record;
}

TVector<TLogRecordPtr> TLogRecordChain::GetReadyRun(
    ui64 afterLsn,
    ui64 maxRecordCount) const
{
    TVector<TLogRecordPtr> records;

    with_lock (Lock) {
        auto recordCount = maxRecordCount > 0
                               ? Min<size_t>(maxRecordCount, Records.size())
                               : Records.size();

        records.reserve(recordCount);

        ui64 tailLsn = afterLsn;
        while (records.size() < recordCount) {
            auto it = Records.find(tailLsn);
            if (it == Records.end() || !it->second.Ready) {
                break;
            }

            records.push_back(it->second.Record);
            tailLsn = it->second.Record->Lsn;
        }
    }

    return records;
}

}   // namespace NCloud::NJournalled
