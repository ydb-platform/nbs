#pragma once

#include "public.h"

#include <util/generic/array_ref.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename TCheck, typename TProcess>
void GroupBy(TArrayRef<T> items, TCheck&& checkGroup, TProcess&& processGroup)
{
    if (!items) {
        return;
    }

    size_t start = 0;
    for (size_t i = 1; i < items.size(); ++i) {
        if (!checkGroup(items[i - 1], items[i])) {
            processGroup(MakeArrayRef(&items[start], i - start));
            start = i;
        }
    }

    processGroup(MakeArrayRef(&items[start], items.size() - start));
}

}   // namespace NCloud::NFileStore::NStorage
