#pragma once

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define ASSERT_VECTORS_EQUAL(expected, actual)                    \
    {                                                             \
        UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size()); \
        for (size_t i = 0; i < expected.size(); ++i) {            \
            UNIT_ASSERT_VALUES_EQUAL(expected[i], actual[i]);     \
        }                                                         \
    }                                                             \
    //  ASSERT_VECTOR_EQUAL

#define ASSERT_VECTORS_CONTENT_EQUAL(expected, actual) \
    {                                                  \
        auto e = expected;                             \
        auto a = actual;                               \
        Sort(e);                                       \
        Sort(a);                                       \
        UNIT_ASSERT_VALUES_EQUAL(e.size(), a.size());  \
        for (size_t i = 0; i < e.size(); ++i) {        \
            UNIT_ASSERT_VALUES_EQUAL(e[i], a[i]);      \
        }                                              \
    }                                                  \
    //  ASSERT_VECTOR_CONTENT_EQUAL

}   // namespace NCloud::NFileStore::NStorage
