#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <util/system/yassert.h>

#include <utility>

namespace NCloud::NFileStore::NClient::NAggregation {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TRow
{
    TVector<TString> Labels;
    T Data;
};

template <typename T>
struct TResult
{
    TVector<TString> Labels;
    T GroupAggregate;
};

template <typename T>
void Aggregate(
    const TVector<TRow<T>>& rows,
    size_t firstLabel,
    TVector<TString>& labels,
    TVector<TResult<T>>& result)
{
    TResult<T> aggregate{labels, {}};
    for (const auto& row: rows) {
        aggregate.GroupAggregate.Add(row.Data);
    }
    result.push_back(std::move(aggregate));

    for (size_t i = firstLabel; i < labels.size(); ++i) {
        THashMap<TString, TVector<TRow<T>>> groups;
        for (const auto& row: rows) {
            groups[row.Labels[i]].push_back(row);
        }

        for (const auto& [label, group]: groups) {
            labels[i] = label;
            Aggregate(group, i + 1, labels, result);
        }
        labels[i].clear();
    }
}

template <typename T>
TVector<TResult<T>> Aggregate(const TVector<TRow<T>>& rows)
{
    if (rows.empty()) {
        return {};
    }

    const size_t labelCount = rows.front().Labels.size();
    for (const auto& row: rows) {
        Y_ABORT_UNLESS(row.Labels.size() == labelCount);
    }

    TVector<TResult<T>> result;
    TVector<TString> labels(labelCount);
    Aggregate(rows, 0, labels, result);
    return result;
}

}   // namespace NCloud::NFileStore::NClient::NAggregation
