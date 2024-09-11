#include "encryptor.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultEncryptionKey = "01234567890123456789012345678901";

////////////////////////////////////////////////////////////////////////////////

bool BlockFilledByZero(const TBlockDataRef& block)
{
    const char* ptr = block.Data();
    return (*ptr == 0) && memcmp(ptr, ptr + 1, block.Size() - 1) == 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEncryptorTest)
{
    Y_UNIT_TEST(AesXtsEncryptorShouldEncryptDecryptBlock)
    {
        ui64 blockIndex = 1234567;

        TString data = TString::TUninitialized(DefaultBlockSize);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<char>(i);
        }

        TString eData = TString::Uninitialized(DefaultBlockSize);
        TString dData = TString::Uninitialized(DefaultBlockSize);

        auto dataRef = TBlockDataRef{ data.data(), data.size() };
        auto eDataRef = TBlockDataRef{ eData.data(), eData.size() };
        auto dDataRef = TBlockDataRef{ dData.data(), dData.size() };

        auto encryptor = CreateAesXtsEncryptor(DefaultEncryptionKey);

        auto err1 = encryptor->Encrypt(dataRef, eDataRef, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err1.GetCode(), err1);
        UNIT_ASSERT(dDataRef.Size() == eDataRef.Size() && eData != data);

        auto err2 = encryptor->Decrypt(eDataRef, dDataRef, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err2.GetCode(), err2);
        UNIT_ASSERT(dData == data);
    }

    Y_UNIT_TEST(AesXtsEncryptorShouldDecryptZeroBlock)
    {
        ui64 blockIndex = 1234567;

        auto eDataRef = TBlockDataRef::CreateZeroBlock(DefaultBlockSize);

        TString tmpData = TString::Uninitialized(DefaultBlockSize);
        auto dDataRef = TBlockDataRef{ tmpData.data(), tmpData.size() };

        auto encryptor = CreateAesXtsEncryptor(DefaultEncryptionKey);

        auto err = encryptor->Decrypt(eDataRef, dDataRef, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err.GetCode(), err);
        UNIT_ASSERT(dDataRef.Size() == eDataRef.Size());
        UNIT_ASSERT(BlockFilledByZero(dDataRef));
    }

    Y_UNIT_TEST(AesXtsEncryptorShouldEncryptBlockUsingBlockIndex)
    {
        ui64 blockIndex1 = 1234567;
        ui64 blockIndex2 = 7654321;

        TString data = TString::TUninitialized(DefaultBlockSize);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<char>(i);
        }

        TString eData1 = TString::Uninitialized(DefaultBlockSize);
        TString eData2 = TString::Uninitialized(DefaultBlockSize);

        auto dataRef = TBlockDataRef{ data.data(), data.size() };
        auto eDataRef1 = TBlockDataRef{ eData1.data(), eData1.size() };
        auto eDataRef2 = TBlockDataRef{ eData2.data(), eData2.size() };

        auto encryptor = CreateAesXtsEncryptor(DefaultEncryptionKey);

        {
            auto err1 = encryptor->Encrypt(dataRef, eDataRef1, blockIndex1);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err1.GetCode(), err1);
            auto err2 = encryptor->Encrypt(dataRef, eDataRef2, blockIndex2);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err2.GetCode(), err2);
            UNIT_ASSERT(eData1 != eData2);
        }

        TString dData1 = TString::Uninitialized(DefaultBlockSize);
        TString dData2 = TString::Uninitialized(DefaultBlockSize);

        auto dDataRef1 = TBlockDataRef{ dData1.data(), dData1.size() };
        auto dDataRef2 = TBlockDataRef{ dData2.data(), dData2.size() };

        auto err1 = encryptor->Decrypt(eDataRef1, dDataRef1, blockIndex1);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err1.GetCode(), err1);
        UNIT_ASSERT(dData1 == data);

        auto err2 = encryptor->Decrypt(eDataRef2, dDataRef2, blockIndex2);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err2.GetCode(), err2);
        UNIT_ASSERT(dData2 == data);
    }

    Y_UNIT_TEST(CaesarEncryptorShouldEncryptDecryptBlock)
    {
        TString data = TString::TUninitialized(DefaultBlockSize);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<char>(i);
        }

        TString eData = TString::Uninitialized(DefaultBlockSize);
        TString dData = TString::Uninitialized(DefaultBlockSize);

        auto dataRef = TBlockDataRef{ data.data(), data.size() };
        auto eDataRef = TBlockDataRef{ eData.data(), eData.size() };
        auto dDataRef = TBlockDataRef{ dData.data(), dData.size() };

        auto blockIndex = 13;
        auto encryptor = CreateTestCaesarEncryptor(42);

        auto err1 = encryptor->Encrypt(dataRef, eDataRef, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err1.GetCode(), err1);
        UNIT_ASSERT(dDataRef.Size() == eDataRef.Size() && eData != data);

        auto err2 = encryptor->Decrypt(eDataRef, dDataRef, blockIndex);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err2.GetCode(), err2);
        UNIT_ASSERT(dData == data);
    }
}

}   // namespace NCloud::NBlockStore
