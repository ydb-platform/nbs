#include "log_chain.h"

#include <util/generic/utility.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

void TLogRecordChain::InitLastErasedLsn(ui64 lsn)
{
    with_lock (Lock) {
        LastErasedLsn = lsn;
    }
}

TResultOrError<TLogRecordPtr> TLogRecordChain::Insert(TLogRecordPtr record)
{
    if (record->PrevLsn >= record->Lsn) {
        return MakeError(E_ARGUMENT);
    }

    with_lock (Lock) {
        if (record->Lsn <= LastErasedLsn) {
            auto erasedStub = std::make_shared<TLogRecord>();
            erasedStub->Lsn = record->Lsn;
            erasedStub->PrevLsn = record->PrevLsn;
            erasedStub->Ready.store(true);
            erasedStub->Promise =
                NThreading::NewPromise<NCloud::NProto::TError>();
            erasedStub->Promise.SetValue(MakeError(S_ALREADY));
            return erasedStub;
        }

        auto nextIt = Records.upper_bound(record->Lsn);
        if (nextIt != Records.end()) {
            const auto& next = *nextIt->second;
            if (record->Lsn > next.PrevLsn) {
                return MakeError(E_INVALID_STATE);
            }
        }

        if (nextIt != Records.begin()) {
            auto prevIt = std::prev(nextIt);
            const auto& prev = *prevIt->second;

            if (prev.Lsn == record->Lsn && prev.PrevLsn == record->PrevLsn) {
                return prevIt->second;
            }

            if (prev.Lsn > record->PrevLsn) {
                return MakeError(E_INVALID_STATE);
            }
        }

        Records.emplace_hint(nextIt, record->Lsn, record);
    }

    return record;
}

TLogRecordPtr TLogRecordChain::Erase(ui64 lsn)
{
    with_lock (Lock) {
        auto it = Records.find(lsn);
        if (it == Records.end()) {
            return nullptr;
        }

        auto record = std::move(it->second);
        Records.erase(it);
        return record;
    }
}

TVector<TLogRecordPtr> TLogRecordChain::EraseTo(ui64 lsn)
{
    TVector<TLogRecordPtr> records;

    with_lock (Lock) {
        auto it = Records.begin();
        while (it != Records.end() && it->second->Lsn <= lsn) {
            records.push_back(std::move(it->second));
            it = Records.erase(it);
        }
        LastErasedLsn = Max(LastErasedLsn, lsn);
    }

    return records;
}

TLogRecordPtr TLogRecordChain::Front() const
{
    with_lock (Lock) {
        return Records.empty() ? nullptr : Records.begin()->second;
    }
}

TLogRecordPtr TLogRecordChain::GetNext(ui64 lsn) const
{
    with_lock (Lock) {
        auto nextIt = Records.upper_bound(lsn);
        if (nextIt == Records.end() || nextIt->second->PrevLsn != lsn) {
            return nullptr;
        }

        return nextIt->second;
    }
}

TVector<TLogRecordPtr> TLogRecordChain::GetReadyTail(
    ui64 afterLsn,
    ui64 maxRecordCnt) const
{
    TVector<TLogRecordPtr> records;

    with_lock (Lock) {
        auto recordCnt = maxRecordCnt > 0
            ? Min<size_t>(maxRecordCnt, Records.size())
            : Records.size();

        records.reserve(recordCnt);

        ui64 tailLsn = afterLsn;
        auto it = Records.upper_bound(tailLsn);
        for (; it != Records.end() && records.size() < recordCnt; ++it) {
            const auto& record = it->second;
            if (record->PrevLsn != tailLsn || !record->Ready.load()) {
                break;
            }

            records.push_back(record);
            tailLsn = record->Lsn;
        }
    }

    return records;
}

}   // namespace NCloud::NJournalled
