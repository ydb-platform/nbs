#include "mount_token.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMountTokenTest)
{
    Y_UNIT_TEST(EmptyMountToken)
    {
        TMountToken token;
        UNIT_ASSERT_VALUES_EQUAL(token.ToString(), "");
        UNIT_ASSERT_VALUES_EQUAL(
            token.ParseString(""),
            TMountToken::EStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(token.Format, TMountToken::EFormat::EMPTY);
        UNIT_ASSERT_VALUES_EQUAL(token.VerifySecret(""), true);
        UNIT_ASSERT_VALUES_EQUAL(token.VerifySecret("my secret"), false);
        UNIT_ASSERT_VALUES_EQUAL(token.VerifySecret("something else"), false);
    }

    Y_UNIT_TEST(Sha384FixedSalt)
    {
        TMountToken token;
        token.SetSecret(
            TMountToken::EFormat::SHA384_V1,
            "my secret",
            "thisismysaltsalt");
        TString result = token.ToString();
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "SHA384_V1:dGhpc2lzbXlzYWx0c2FsdA==:xv6l5+"
            "67hlFaHEAW0BY1XWd75LlUB8TvDrmTjOIRMqeOHj/o9+ey2uFizV9h0K0O");
        TMountToken token2;
        UNIT_ASSERT_VALUES_EQUAL(
            token2.ParseString(result),
            TMountToken::EStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(token.Format, token2.Format);
        UNIT_ASSERT_VALUES_EQUAL(token.Salt, token2.Salt);
        UNIT_ASSERT_VALUES_EQUAL(token.Hash, token2.Hash);
        UNIT_ASSERT_VALUES_EQUAL(token.VerifySecret(""), false);
        UNIT_ASSERT_VALUES_EQUAL(token.VerifySecret("my secret"), true);
        UNIT_ASSERT_VALUES_EQUAL(token.VerifySecret("something else"), false);
    }

    Y_UNIT_TEST(Sha384RandomSalt)
    {
        TMountToken token;
        token.SetSecret(TMountToken::EFormat::SHA384_V1, "my secret");
        UNIT_ASSERT_VALUES_EQUAL(token.Salt.size(), 16);
        TString result = token.ToString();
        TMountToken token2;
        UNIT_ASSERT_VALUES_EQUAL(
            token2.ParseString(result),
            TMountToken::EStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(token.Format, token2.Format);
        UNIT_ASSERT_VALUES_EQUAL(token.Salt, token2.Salt);
        UNIT_ASSERT_VALUES_EQUAL(token.Hash, token2.Hash);
        UNIT_ASSERT_VALUES_EQUAL(token.VerifySecret(""), false);
        UNIT_ASSERT_VALUES_EQUAL(token.VerifySecret("my secret"), true);
        UNIT_ASSERT_VALUES_EQUAL(token.VerifySecret("something else"), false);
    }

    Y_UNIT_TEST(Sha384ParseErrors)
    {
        TMountToken token;
        UNIT_ASSERT_VALUES_EQUAL(
            token.ParseString("foo"),
            TMountToken::EStatus::FORMAT_UNKNOWN);
        UNIT_ASSERT_VALUES_EQUAL(
            token.ParseString("SHA384_V1:"),
            TMountToken::EStatus::TOKEN_CORRUPTED);
        UNIT_ASSERT_VALUES_EQUAL(
            token.ParseString("SHA384_V1:+++:+++"),
            TMountToken::EStatus::TOKEN_CORRUPTED);
        UNIT_ASSERT_VALUES_EQUAL(
            token.ParseString("SHA384_V1::"),
            TMountToken::EStatus::TOKEN_CORRUPTED);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
