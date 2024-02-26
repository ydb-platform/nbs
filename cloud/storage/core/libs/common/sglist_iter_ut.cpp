#include "sglist_iter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TA
{
    uint16_t a;
    uint32_t b;
};

struct TB
{
    uint64_t c;
};

struct TC
{
    char d;
};

struct TTest: public TA, public TB, public TC
{};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSgListIteratorTest)
{
    Y_UNIT_TEST(ShouldRead)
    {
        TVector<char> buffer(128, 0);

        TSgList sgList;
        {
            TSgListInputIterator iter(sgList);
            size_t read = iter.Read(buffer.data(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(read, 0);
            UNIT_ASSERT_VALUES_EQUAL(buffer[0], 0);
        }


        TVector<TString> data = {"ab", "cd", "efg", "h"};
        for (const auto& str: data) {
            sgList.emplace_back(str.data(), str.size());
        }

        {
            TSgListInputIterator iter(sgList);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 8);

            size_t read = iter.Read(buffer.data(), 3);
            UNIT_ASSERT_VALUES_EQUAL(read, 3);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(buffer.data(), 3), "abc");

            read = iter.Read(buffer.data() + 3, 3);
            UNIT_ASSERT_VALUES_EQUAL(read, 3);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(buffer.data() + 3, 3), "def");

            read = iter.Read(buffer.data() + 6, 3);
            UNIT_ASSERT_VALUES_EQUAL(read, 2);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(buffer.data() + 6, 2), "gh");

            read = iter.Read(buffer.data(), 3);
            UNIT_ASSERT_VALUES_EQUAL(read, 0);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 0);
        }

        {
            TSgListInputIterator iter(sgList);
            size_t read = iter.Read(buffer.data(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(read, 8);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(buffer.data(), read), "abcdefgh");

            read = iter.Read(buffer.data(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(read, 0);
        }
    }

    Y_UNIT_TEST(ShouldReadStructs)
    {
        TVector<char> buffer(128, 0);

        TTest test = {{100, 500}, {500100}, {'A'}};

        TSgList sgList;
        sgList.emplace_back(reinterpret_cast<char*>(&test), sizeof(test));

        TTest other = {};
        TSgListInputIterator iter(sgList);
        UNIT_ASSERT(iter.Read(other));
        UNIT_ASSERT_VALUES_EQUAL(test.a, other.a);
        UNIT_ASSERT_VALUES_EQUAL(test.b, other.b);
        UNIT_ASSERT_VALUES_EQUAL(test.c, other.c);
        UNIT_ASSERT_VALUES_EQUAL(test.d, other.d);

        other = {};
        UNIT_ASSERT(!iter.Read(other));
    }

    Y_UNIT_TEST(ShouldReadData)
    {
        TStringBuf buffer("\0abc\0def\0\0", 10);

        TSgList sgList;
        sgList.emplace_back(&buffer[0], 3);
        sgList.emplace_back(&buffer[0] + 3, 3);
        sgList.emplace_back(&buffer[0] + 6, 4);

        TString s1{"dummy"}, s2, s3, s4{"dummy"};

        TSgListInputIterator iter(sgList);
        UNIT_ASSERT(iter.ReadAll(s1, s2, s3, s4));

        UNIT_ASSERT_VALUES_EQUAL(s1.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(s2, "abc");
        UNIT_ASSERT_VALUES_EQUAL(s3, "def");
        UNIT_ASSERT_VALUES_EQUAL(s4.size(), 0);

        UNIT_ASSERT(!iter.Read(s1));
    }

    Y_UNIT_TEST(ShouldWrite)
    {
        TVector<char> buffer(64, '\n');

        TSgList sgList;
        sgList.emplace_back(&buffer[0], 3);
        sgList.emplace_back(&buffer[10], 3);
        sgList.emplace_back(&buffer[20], 3);
        sgList.emplace_back(&buffer[30], 3);

        {
            TSgListOutputIterator iter(sgList);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 12);

            size_t written = iter.Write("abcdef", 6);

            UNIT_ASSERT_VALUES_EQUAL(written, 6);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 6);
            UNIT_ASSERT_VALUES_EQUAL("abc", sgList[0].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("def", sgList[1].AsStringBuf());

            written = iter.Write("uvwx", 4);
            UNIT_ASSERT_VALUES_EQUAL(written, 4);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 2);
            written = iter.Write("yz", 2);
            UNIT_ASSERT_VALUES_EQUAL(written, 2);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 0);

            UNIT_ASSERT_VALUES_EQUAL("uvw", sgList[2].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("xyz", sgList[3].AsStringBuf());

            written = iter.Write("uvwx", 4);
            UNIT_ASSERT_VALUES_EQUAL(written, 0);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 0);
        }

        {
            TSgListOutputIterator iter(sgList);
            size_t written = iter.Write("zyxwvufedcba", 12);
            UNIT_ASSERT_VALUES_EQUAL(written, 12);

            UNIT_ASSERT_VALUES_EQUAL("zyx", sgList[0].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("wvu", sgList[1].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("fed", sgList[2].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("cba", sgList[3].AsStringBuf());

            written = iter.Write("zyxwvufedcba", 12);
            UNIT_ASSERT_VALUES_EQUAL(written, 0);
        }
    }

    Y_UNIT_TEST(ShouldWriteStruct)
    {
        TVector<char> buffer(sizeof(TTest) + 1, 0);

        TTest test = {{100, 500}, {500100}, {'A'}};

        TSgList sgList;
        sgList.emplace_back(&buffer[0], buffer.size());

        TSgListOutputIterator iter(sgList);
        UNIT_ASSERT(iter.Write(test));

        TTest& other = *reinterpret_cast<TTest*>(&buffer[0]);
        UNIT_ASSERT_VALUES_EQUAL(test.a, other.a);
        UNIT_ASSERT_VALUES_EQUAL(test.b, other.b);
        UNIT_ASSERT_VALUES_EQUAL(test.c, other.c);
        UNIT_ASSERT_VALUES_EQUAL(test.d, other.d);

        UNIT_ASSERT(!iter.Write(test));
    }

    Y_UNIT_TEST(ShouldWriteData)
    {
        TVector<char> buffer(64, 'z');

        TSgList sgList;
        sgList.emplace_back(&buffer[0], 3);
        sgList.emplace_back(&buffer[10], 3);
        sgList.emplace_back(&buffer[20], 3);
        sgList.emplace_back(&buffer[30], 3);

        {
            TSgListOutputIterator iter(sgList);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 12);

            UNIT_ASSERT(iter.Write(TString("abcdef")));

            UNIT_ASSERT_VALUES_EQUAL("abc", sgList[0].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("def", sgList[1].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL('\0', sgList[2].AsStringBuf()[0]);
            UNIT_ASSERT_VALUES_EQUAL(iter.Size(), 5);

            UNIT_ASSERT(!iter.Write(TString("abcdefabcdefabcdef")));
        }

        {
            TStringBuf data = "zyxwvufedcb";

            TSgListOutputIterator iter(sgList);

            UNIT_ASSERT(iter.Write(data));
            UNIT_ASSERT_VALUES_EQUAL("zyx", sgList[0].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("wvu", sgList[1].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("fed", sgList[2].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf("cb\0", 3),  sgList[3].AsStringBuf());

            UNIT_ASSERT(!iter.Write(data));
        }

        {
            TStringBuf data = "zyxwvufedcba";

            TSgListOutputIterator iter(sgList);

            // no space for \0
            UNIT_ASSERT(!iter.Write(data));
        }

        {
            TSgListOutputIterator iter(sgList);

            UNIT_ASSERT(iter.WriteAll(TString{}, TString{}, TString{}));
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf("\0\0\0", 3), sgList[0].AsStringBuf());
        }
    }

    Y_UNIT_TEST(ShouldReadAndWriteStructs)
    {
        TVector<char> buffer(sizeof(TTest) + 1, 0);

        TSgList sgList;
        sgList.emplace_back(&buffer[0], buffer.size());

        TTest test = {{100, 500}, {500100}, {'A'}};

        {
            TA& a = test;
            TB& b = test;
            TC& c = test;

            TSgListOutputIterator iter(sgList);
            UNIT_ASSERT(iter.WriteAll(a, b, c));
        }

        TTest other;
        {
            TA& a = other;
            TB& b = other;
            TC& c = other;

            TSgListInputIterator iter(sgList);
            UNIT_ASSERT(iter.ReadAll(a, b, c));
        }

        UNIT_ASSERT_VALUES_EQUAL(test.a, other.a);
        UNIT_ASSERT_VALUES_EQUAL(test.b, other.b);
        UNIT_ASSERT_VALUES_EQUAL(test.c, other.c);
        UNIT_ASSERT_VALUES_EQUAL(test.d, other.d);

        {
            TSgListOutputIterator iter(sgList);
            UNIT_ASSERT(!iter.WriteAll(test, test));
        }

        {
            TSgListInputIterator iter(sgList);
            UNIT_ASSERT(!iter.ReadAll(other, other));
        }
    }
}

}   // namespace NCloud
