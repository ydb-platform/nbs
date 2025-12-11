#pragma once

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define ASSERT_MAP_EQUAL(li, ri)                               \
    {                                                          \
        auto l = li;                                           \
        auto r = ri;                                           \
        UNIT_ASSERT_VALUES_EQUAL(l.size(), r.size());          \
        for (auto lItr = l.begin(); lItr != l.end(); ++lItr) { \
            UNIT_ASSERT_VALUES_EQUAL_C(                        \
                lItr->second,                                  \
                r[lItr->first],                                \
                lItr->first);                                  \
        }                                                      \
        UNIT_ASSERT_VALUES_EQUAL(l.size(), r.size());          \
    }                                                          \
    // ASSERT_MAP_EQUAL

#define ASSERT_VECTORS_EQUAL(li, ri)                                 \
    [](const auto& l, const auto& r)                                 \
    {                                                                \
        for (size_t i = 0; i < Min<ui32>(l.size(), r.size()); ++i) { \
            UNIT_ASSERT_VALUES_EQUAL_C(l[i], r[i], i);               \
        }                                                            \
        UNIT_ASSERT_VALUES_EQUAL(l.size(), r.size());                \
    }((li), (ri))   // ASSERT_VECTORS_EQUAL

#define ASSERT_VECTOR_CONTENTS_EQUAL(li, ri)                         \
    {                                                                \
        auto l = li;                                                 \
        auto r = ri;                                                 \
        Sort(l);                                                     \
        Sort(r);                                                     \
        for (size_t i = 0; i < Min<ui32>(l.size(), r.size()); ++i) { \
            UNIT_ASSERT_VALUES_EQUAL_C(l[i], r[i], i);               \
        }                                                            \
        UNIT_ASSERT_VALUES_EQUAL(l.size(), r.size());                \
    }                                                                \
    // ASSERT_VECTOR_CONTENTS_EQUAL

#define ASSERT_PARTITION2_BLOCKS_EQUAL_C(l, r, c)                \
    UNIT_ASSERT_VALUES_EQUAL_C(l.BlockIndex, r.BlockIndex, c);   \
    UNIT_ASSERT_VALUES_EQUAL_C(l.MinCommitId, r.MinCommitId, c); \
    UNIT_ASSERT_VALUES_EQUAL_C(l.MaxCommitId, r.MaxCommitId, c); \
    UNIT_ASSERT_VALUES_EQUAL_C(l.Zeroed, r.Zeroed, c);           \
    // ASSERT_PARTITION2_BLOCKS_EQUAL

#define ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(l, r)        \
    UNIT_ASSERT_VALUES_EQUAL(l.size(), r.size());        \
    for (size_t i = 0; i < l.size(); ++i) {              \
        ASSERT_PARTITION2_BLOCKS_EQUAL_C(l[i], r[i], i); \
    }                                                    \
    // ASSERT_PARTITION2_BLOCK_LISTS_EQUAL

}   // namespace NCloud::NBlockStore::NStorage
