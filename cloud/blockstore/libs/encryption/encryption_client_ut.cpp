#include "encryption_client.h"

#include "encryptor.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool BlockFilledByValue(const TBlockDataRef& block, char value)
{
    const char* ptr = block.Data();
    return (*ptr == value) && memcmp(ptr, ptr + 1, block.Size() - 1) == 0;
}

////////////////////////////////////////////////////////////////////////////////

NProto::TEncryptionDesc GetDefaultEncryption()
{
    NProto::TEncryptionDesc encryption;
    encryption.SetMode(NProto::ENCRYPTION_AES_XTS);
    encryption.SetKeyHash("testKeyHash");
    return encryption;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestEncryptionKeyProvider final
    : IEncryptionKeyProvider
{
    TFuture<TResponse> GetKey(
        const NProto::TEncryptionSpec& spec,
        const TString& diskId) override
    {
        Y_UNUSED(diskId);

        return MakeFuture<TResultOrError<TEncryptionKey>>(
            TEncryptionKey{spec.GetKeyPath().GetKmsKey().GetEncryptedDEK()});
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEncryptor final
    : public IEncryptor
{
    NProto::TError Encrypt(
        TBlockDataRef srcRef,
        TBlockDataRef dstRef,
        ui64 blockIndex) override
    {
        UNIT_ASSERT(srcRef.Size() == dstRef.Size());
        UNIT_ASSERT(srcRef.Data() != nullptr);

        const char* srcPtr = srcRef.Data();
        char* dstPtr = const_cast<char*>(dstRef.Data());

        for (size_t i = 0; i < srcRef.Size(); ++i) {
            *dstPtr = *srcPtr + static_cast<char>(blockIndex);
            ++srcPtr;
            ++dstPtr;
        }

        return {};
    }

    NProto::TError Decrypt(
        TBlockDataRef srcRef,
        TBlockDataRef dstRef,
        ui64 blockIndex) override
    {
        UNIT_ASSERT(srcRef.Size() == dstRef.Size());

        if (srcRef.Data() == nullptr) {
            memset(const_cast<char*>(dstRef.Data()), 0, srcRef.Size());
            return {};
        }

        const char* srcPtr = srcRef.Data();
        char* dstPtr = const_cast<char*>(dstRef.Data());

        for (size_t i = 0; i < srcRef.Size(); ++i) {
            *dstPtr = *srcPtr - static_cast<char>(blockIndex);
            ++srcPtr;
            ++dstPtr;
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const TString EncryptionKey = "01234567891011121314151617181920";
    const TString KekId = "nbs";
    static constexpr ui32 BlockSize = 4_KB;

    IEncryptorPtr Encryptor;
    ILoggingServicePtr Logging;
    IEncryptionKeyProviderPtr KeyProvider;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        Encryptor = CreateAesXtsEncryptor(EncryptionKey);
        Logging = CreateLoggingService("console");
        KeyProvider = std::make_shared<TTestEncryptionKeyProvider>();
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TMountVolumeResponse MountVolume(IBlockStore& client)
{
    auto future = client.MountVolume(
        MakeIntrusive<TCallContext>(),
        std::make_shared<NProto::TMountVolumeRequest>());
    return future.GetValue(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
int GetFieldCount()
{
    return T::GetDescriptor()->field_count();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEncryptionClientTest)
{
    Y_UNIT_TEST(ShouldAddEncryptionKeyHashToMount)
    {
        auto logging = CreateLoggingService("console");

        auto testClient = std::make_shared<TTestService>();
        auto encryptionDesc = GetDefaultEncryption();

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                const auto& spec = request->GetEncryptionSpec();
                UNIT_ASSERT(encryptionDesc.GetMode() == spec.GetMode());
                UNIT_ASSERT(encryptionDesc.GetKeyHash() == spec.GetKeyHash());
                return MakeFuture<NProto::TMountVolumeResponse>();
            };

        {
            auto encryptionClient = CreateEncryptionClient(
                testClient,
                logging,
                nullptr,
                encryptionDesc);

            auto mountResponse = MountVolume(*encryptionClient);
            UNIT_ASSERT(!HasError(mountResponse));
        }

        {
            auto encryptionClient = CreateSnapshotEncryptionClient(
                testClient,
                logging,
                encryptionDesc);

            auto mountResponse = MountVolume(*encryptionClient);
            UNIT_ASSERT(!HasError(mountResponse));
        }
    }

    Y_UNIT_TEST(ShouldFailMountIfOtherEncryptionClientExists)
    {
        auto logging = CreateLoggingService("console");
        auto encryptionDesc = GetDefaultEncryption();

        auto testClient = std::make_shared<TTestService>();

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                const auto& spec = request->GetEncryptionSpec();
                UNIT_ASSERT(encryptionDesc.GetMode() == spec.GetMode());
                UNIT_ASSERT(encryptionDesc.GetKeyHash() == spec.GetKeyHash());
                return MakeFuture<NProto::TMountVolumeResponse>();
            };

        {
            auto encryptionClient1 = CreateEncryptionClient(
                testClient,
                logging,
                nullptr,
                encryptionDesc);

            auto mountResponse1 = MountVolume(*encryptionClient1);
            UNIT_ASSERT(!HasError(mountResponse1));

            auto encryptionClient2 = CreateEncryptionClient(
                encryptionClient1,
                logging,
                nullptr,
                encryptionDesc);

            auto mountResponse2 = MountVolume(*encryptionClient2);
            UNIT_ASSERT(HasError(mountResponse2));
        }

        {
            auto encryptionClient1 = CreateSnapshotEncryptionClient(
                testClient,
                logging,
                encryptionDesc);

            auto mountResponse1 = MountVolume(*encryptionClient1);
            UNIT_ASSERT(!HasError(mountResponse1));

            auto encryptionClient2 = CreateSnapshotEncryptionClient(
                encryptionClient1,
                logging,
                encryptionDesc);

            auto mountResponse2 = MountVolume(*encryptionClient2);
            UNIT_ASSERT(!HasError(mountResponse2));
        }
    }

    Y_UNIT_TEST(ShouldEncryptAllBlocksInWriteBlocks)
    {
        auto logging = CreateLoggingService("console");
        int blockSize = 8;
        int storageBlocksCount = 16;

        TVector<TString> storageBlocks(Reserve(storageBlocksCount));
        for (int i = 0; i < storageBlocksCount; ++i) {
            storageBlocks.emplace_back(blockSize, '0');
        }

        auto testClient = std::make_shared<TTestService>();
        auto testEncryptor = std::make_shared<TTestEncryptor>();

        auto encryptionClient = CreateEncryptionClient(
            testClient,
            logging,
            testEncryptor,
            GetDefaultEncryption());

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);

                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetBlockSize(blockSize);
                return MakeFuture(std::move(response));
            };

        testClient->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest> request) {
                const auto& buffers = request->GetBlocks().GetBuffers();

                for (int i = 0; i < buffers.size(); ++i) {
                    storageBlocks[i] = buffers[i];
                }

                return MakeFuture(NProto::TWriteBlocksResponse());
            };

        auto mountResponse = MountVolume(*encryptionClient);
        UNIT_ASSERT(!HasError(mountResponse));

        int blocksCount = 7;

        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        auto buffers = request->MutableBlocks()->MutableBuffers();
        for (int i = 0; i < blocksCount; ++i) {
            auto& buffer = *buffers->Add();
            buffer = TString(blockSize, 'a' + i);
        }

        auto future = encryptionClient->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        for (int i = 0; i < storageBlocksCount; ++i) {
            TBlockDataRef block(storageBlocks[i].data(), storageBlocks[i].size());

            if (i < blocksCount) {
                TString decrypted(block.Size(), 0);
                TBlockDataRef decryptedRef(decrypted.data(), decrypted.size());
                auto err = testEncryptor->Decrypt(block, decryptedRef, i);
                UNIT_ASSERT_EQUAL_C(S_OK, err.GetCode(), err);
                UNIT_ASSERT(BlockFilledByValue(decryptedRef, 'a' + i));
            } else {
                UNIT_ASSERT(BlockFilledByValue(block, '0'));
            }
        }
    }

    Y_UNIT_TEST(ShouldEncryptAllBlocksInWriteBlockLocal)
    {
        auto logging = CreateLoggingService("console");
        int blockSize = 8;
        int storageBlocksCount = 16;

        TVector<TString> storageBlocks(Reserve(storageBlocksCount));
        for (int i = 0; i < storageBlocksCount; ++i) {
            storageBlocks.emplace_back(blockSize, '0');
        }

        auto testClient = std::make_shared<TTestService>();
        auto testEncryptor = std::make_shared<TTestEncryptor>();

        auto encryptionClient = CreateEncryptionClient(
            testClient,
            logging,
            testEncryptor,
            GetDefaultEncryption());

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);

                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetBlockSize(blockSize);
                return MakeFuture(std::move(response));
            };

        testClient->WriteBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                const auto& sglist = guard.Get();

                for (size_t i = 0; i < sglist.size(); ++i) {
                    UNIT_ASSERT(storageBlocks[i].size() == sglist[i].Size());
                    auto* dst = const_cast<char*>(storageBlocks[i].data());
                    auto* src = sglist[i].Data();
                    memcpy(dst, src, sglist[i].Size());
                }

                return MakeFuture(NProto::TWriteBlocksLocalResponse());
            };

        auto mountResponse = MountVolume(*encryptionClient);
        UNIT_ASSERT(!HasError(mountResponse));

        int blocksCount = 7;

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(blocks, blocksCount, TString(blockSize, 0));
        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request->BlocksCount = blocksCount;
        request->BlockSize = blockSize;
        request->Sglist = TGuardedSgList(sglist);

        for (size_t i = 0; i < sglist.size(); ++i) {
            auto& block = sglist[i];
            memset(const_cast<char*>(block.Data()), 'a' + i, block.Size());
        }

        auto future = encryptionClient->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        for (int i = 0; i < storageBlocksCount; ++i) {
            TBlockDataRef block(storageBlocks[i].data(), storageBlocks[i].size());

            if (i < blocksCount) {
                TString decrypted(block.Size(), 0);
                TBlockDataRef decryptedRef(decrypted.data(), decrypted.size());
                auto err = testEncryptor->Decrypt(block, decryptedRef, i);
                UNIT_ASSERT_EQUAL_C(S_OK, err.GetCode(), err);
                UNIT_ASSERT(BlockFilledByValue(decryptedRef, 'a' + i));
            } else {
                UNIT_ASSERT(BlockFilledByValue(block, '0'));
            }
        }
    }

    Y_UNIT_TEST(ShouldEncryptAllBlocksInZeroBlockUsingWriteBlocksLocal)
    {
        auto logging = CreateLoggingService("console");
        size_t blockSize = 8;
        size_t storageBlocksCount = 16;

        TVector<TString> storageBlocks(Reserve(storageBlocksCount));
        for (size_t i = 0; i < storageBlocksCount; ++i) {
            storageBlocks.emplace_back(blockSize, '0');
        }

        auto testClient = std::make_shared<TTestService>();
        auto testEncryptor = std::make_shared<TTestEncryptor>();

        auto encryptionClient = CreateEncryptionClient(
            testClient,
            logging,
            testEncryptor,
            GetDefaultEncryption());

        auto zRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        zRequest->MutableHeaders()->SetClientId("testClientId");
        zRequest->SetDiskId("testDiskId");
        zRequest->SetStartIndex(1);
        zRequest->SetBlocksCount(6);
        zRequest->SetFlags(42);
        zRequest->SetSessionId("testSessionId");
        UNIT_ASSERT_VALUES_EQUAL(6, GetFieldCount<NProto::TZeroBlocksRequest>());

        NProto::TWriteBlocksLocalResponse wResponse;
        wResponse.MutableError()->SetMessage("testMessage");
        wResponse.MutableTrace()->SetRequestStartTime(42);
        wResponse.SetThrottlerDelay(13);
        UNIT_ASSERT_VALUES_EQUAL(3, GetFieldCount<NProto::TWriteBlocksResponse>());

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);

                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetBlockSize(blockSize);
                return MakeFuture(std::move(response));
            };

        testClient->WriteBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> wRequest) {
                auto guard = wRequest->Sglist.Acquire();
                UNIT_ASSERT(guard);
                const auto& sglist = guard.Get();

                for (size_t i = 0; i < sglist.size(); ++i) {
                    size_t n = i + wRequest->GetStartIndex();
                    UNIT_ASSERT(storageBlocks[n].size() == sglist[i].Size());
                    auto* dst = const_cast<char*>(storageBlocks[n].data());
                    auto* src = sglist[i].Data();
                    memcpy(dst, src, sglist[i].Size());
                }

                UNIT_ASSERT_VALUES_EQUAL(
                    zRequest->MutableHeaders()->GetClientId(),
                    wRequest->MutableHeaders()->GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(
                    zRequest->GetDiskId(),
                    wRequest->GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL(
                    zRequest->GetStartIndex(),
                    wRequest->GetStartIndex());
                UNIT_ASSERT_VALUES_EQUAL(
                    zRequest->GetFlags(),
                    wRequest->GetFlags());
                UNIT_ASSERT_VALUES_EQUAL(
                    zRequest->GetSessionId(),
                    wRequest->GetSessionId());
                UNIT_ASSERT_VALUES_EQUAL(6, GetFieldCount<NProto::TZeroBlocksRequest>());
                UNIT_ASSERT_VALUES_EQUAL(6, GetFieldCount<NProto::TWriteBlocksRequest>());

                return MakeFuture(wResponse);
            };

        auto mountResponse = MountVolume(*encryptionClient);
        UNIT_ASSERT(!HasError(mountResponse));

        auto future = encryptionClient->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            zRequest);
        auto zResponse = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(zResponse));

        UNIT_ASSERT_VALUES_EQUAL(
            wResponse.MutableError()->GetMessage(),
            zResponse.MutableError()->GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            wResponse.MutableTrace()->GetRequestStartTime(),
            zResponse.MutableTrace()->GetRequestStartTime());
        UNIT_ASSERT_VALUES_EQUAL(
            wResponse.GetThrottlerDelay(),
            zResponse.GetThrottlerDelay());
        UNIT_ASSERT_VALUES_EQUAL(3, GetFieldCount<NProto::TWriteBlocksResponse>());
        UNIT_ASSERT_VALUES_EQUAL(3, GetFieldCount<NProto::TZeroBlocksResponse>());

        for (size_t i = 0; i < storageBlocksCount; ++i) {
            TBlockDataRef block(storageBlocks[i].data(), storageBlocks[i].size());

            if (zRequest->GetStartIndex() <= i &&
                i < zRequest->GetStartIndex() + zRequest->GetBlocksCount())
            {
                TString decrypted(block.Size(), 0);
                TBlockDataRef decryptedRef(decrypted.data(), decrypted.size());
                auto err = testEncryptor->Decrypt(block, decryptedRef, i);
                UNIT_ASSERT_EQUAL_C(S_OK, err.GetCode(), err);
                UNIT_ASSERT(BlockFilledByValue(decryptedRef, 0));
            } else {
                UNIT_ASSERT(BlockFilledByValue(block, '0'));
            }
        }
    }

    Y_UNIT_TEST(ShouldDecryptAllBlocksInReadBlocksWhenBitmaskIsEmpty)
    {
        auto logging = CreateLoggingService("console");
        const int blockSize = 4;
        const int blocksCount = 16;

        auto testClient = std::make_shared<TTestService>();
        auto testEncryptor = std::make_shared<TTestEncryptor>();

        auto encryptionClient = CreateEncryptionClient(
            testClient,
            logging,
            testEncryptor,
            GetDefaultEncryption());

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);

                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetBlockSize(blockSize);
                return MakeFuture(std::move(response));
            };

        testClient->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                NProto::TReadBlocksResponse response;

                auto sglist = ResizeIOVector(
                    *response.MutableBlocks(),
                    request->GetBlocksCount(),
                    blockSize);

                for (size_t i = 0; i < request->GetBlocksCount(); ++i) {
                    TString buf(blockSize, 'a' + i);
                    TBlockDataRef bufRef(buf.data(), buf.size());
                    auto err = testEncryptor->Encrypt(bufRef, sglist[i], i);
                    UNIT_ASSERT_EQUAL_C(S_OK, err.GetCode(), err);
                }

                return MakeFuture(std::move(response));
            };

        auto mountResponse = MountVolume(*encryptionClient);
        UNIT_ASSERT(!HasError(mountResponse));

        auto request = std::make_shared<NProto::TReadBlocksRequest>();
        request->SetStartIndex(0);
        request->SetBlocksCount(blocksCount);

        auto future = encryptionClient->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        const auto& buffers = response.GetBlocks().GetBuffers();
        for (int i = 0; i < blocksCount; ++i) {
            TBlockDataRef block(buffers[i].data(), buffers[i].size());
            UNIT_ASSERT(BlockFilledByValue(block, 'a' + i));
        }
    }

    Y_UNIT_TEST(ShouldSkipDecryptionForZeroBlocks)
    {
        auto logging = CreateLoggingService("console");
        const int blockSize = 4;
        const int blocksCount = 16;

        auto testClient = std::make_shared<TTestService>();
        auto testEncryptor = std::make_shared<TTestEncryptor>();

        auto encryptionClient = CreateEncryptionClient(
            testClient,
            logging,
            testEncryptor,
            GetDefaultEncryption());

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);

                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetBlockSize(blockSize);
                return MakeFuture(std::move(response));
            };

        testClient->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                NProto::TReadBlocksResponse response;

                auto sglist = ResizeIOVector(
                    *response.MutableBlocks(),
                    request->GetBlocksCount(),
                    blockSize);

                for (size_t i = 0; i < request->GetBlocksCount(); ++i) {
                    std::memset(
                        const_cast<char*>(sglist[i].Data()),
                        0,
                        sglist[i].Size());
                }

                return MakeFuture(std::move(response));
            };

        auto mountResponse = MountVolume(*encryptionClient);
        UNIT_ASSERT(!HasError(mountResponse));

        auto request = std::make_shared<NProto::TReadBlocksRequest>();
        request->SetStartIndex(0);
        request->SetBlocksCount(blocksCount);

        auto future = encryptionClient->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        const auto& buffers = response.GetBlocks().GetBuffers();
        for (int i = 0; i < blocksCount; ++i) {
            TBlockDataRef block(buffers[i].data(), buffers[i].size());
            UNIT_ASSERT(BlockFilledByValue(block, 0u));
        }
    }

    Y_UNIT_TEST(ShouldDecryptAllBlocksInReadBlocksLocalWhenBitmaskIsEmpty)
    {
        auto logging = CreateLoggingService("console");
        const int blockSize = 4;
        const int blocksCount = 16;

        auto testClient = std::make_shared<TTestService>();
        auto testEncryptor = std::make_shared<TTestEncryptor>();

        auto encryptionClient = CreateEncryptionClient(
            testClient,
            logging,
            testEncryptor,
            GetDefaultEncryption());

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);

                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetBlockSize(blockSize);
                return MakeFuture(std::move(response));
            };

        testClient->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                auto& sglist = guard.Get();

                for (size_t i = 0; i < request->GetBlocksCount(); ++i) {
                    TString buf(blockSize, 'a' + i);
                    TBlockDataRef bufRef(buf.data(), buf.size());
                    auto err = testEncryptor->Encrypt(bufRef, sglist[i], i);
                    UNIT_ASSERT_EQUAL_C(S_OK, err.GetCode(), err);
                }

                return MakeFuture(NProto::TReadBlocksLocalResponse());
            };

        auto mountResponse = MountVolume(*encryptionClient);
        UNIT_ASSERT(!HasError(mountResponse));

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(blocks, blocksCount, TString(blockSize, 0));
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetStartIndex(0);
        request->SetBlocksCount(blocksCount);
        request->BlockSize = blockSize;
        request->Sglist = TGuardedSgList(sglist);

        auto future = encryptionClient->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        for (size_t i = 0; i < sglist.size(); ++i) {
            UNIT_ASSERT(BlockFilledByValue(sglist[i], 'a' + i));
        }
    }

    void DoDecryptOverlayBlocksInReadBlocks(NProto::EStorageMediaKind mediaKind)
    {
        auto logging = CreateLoggingService("console");
        const int blockSize = 4;
        const int blocksCount = 16;
        std::array<ui8, blocksCount> encryptedBlockMask
            = {{0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0}};

        auto testClient = std::make_shared<TTestService>();
        auto testEncryptor = std::make_shared<TTestEncryptor>();

        auto encryptionClient = CreateEncryptionClient(
            testClient,
            logging,
            testEncryptor,
            GetDefaultEncryption());

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);

                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetBlockSize(blockSize);
                response.MutableVolume()->SetStorageMediaKind(mediaKind);
                return MakeFuture(std::move(response));
            };

        testClient->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                NProto::TReadBlocksResponse response;

                auto sglist = ResizeIOVector(
                    *response.MutableBlocks(),
                    request->GetBlocksCount(),
                    blockSize);

                TString bitmaskStr(request->GetBlocksCount() / 8 + 1, 0);
                for (size_t i = 0; i < request->GetBlocksCount(); ++i) {
                    if (!encryptedBlockMask[i]) {
                        size_t byte = i / 8;
                        size_t bit = i % 8;
                        bitmaskStr[byte] = (bitmaskStr[byte] | (1 << bit));
                    }

                    char value = 'a' + i;

                    if (encryptedBlockMask[i]) {
                        TString buf(blockSize, value);
                        TBlockDataRef bufRef(buf.data(), buf.size());
                        auto err = testEncryptor->Encrypt(bufRef, sglist[i], i);
                        UNIT_ASSERT_EQUAL_C(S_OK, err.GetCode(), err);
                    } else {
                        memset(
                            const_cast<char*>(sglist[i].Data()),
                            value,
                            blockSize);
                    }
                }

                response.SetUnencryptedBlockMask(bitmaskStr);
                return MakeFuture(std::move(response));
            };

        auto mountResponse = MountVolume(*encryptionClient);
        UNIT_ASSERT(!HasError(mountResponse));

        auto request = std::make_shared<NProto::TReadBlocksRequest>();
        request->SetStartIndex(0);
        request->SetBlocksCount(blocksCount);

        auto future = encryptionClient->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        const auto& buffers = response.GetBlocks().GetBuffers();
        for (int i = 0; i < blocksCount; ++i) {
            if (IsDiskRegistryMediaKind(mediaKind) && !encryptedBlockMask[i]) {
                UNIT_ASSERT(buffers[i].empty());
                continue;
            }

            TBlockDataRef block(buffers[i].data(), buffers[i].size());
            UNIT_ASSERT(BlockFilledByValue(block, 'a' + i));
        }
    }

#define BLOCKSTORE_IMPLEMENT_TEST(mediaKind, name, ...)                        \
    Y_UNIT_TEST(ShouldDecryptOverlayBlocksInReadBlocksFor##name)               \
    {                                                                          \
        DoDecryptOverlayBlocksInReadBlocks(mediaKind);                         \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_TEST

    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD, SSD)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_HYBRID, Hybrid)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_HDD, HDD)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED, SSDNonreplicated)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2, SSDMirror2)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3, SSDMirror3)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL, SSDLocal)

#undef BLOCKSTORE_IMPLEMENT_TEST

    void DoDecryptOverlayBlocksInReadBlocksLocal(NProto::EStorageMediaKind mediaKind)
    {
        auto logging = CreateLoggingService("console");
        const int blockSize = 4;
        const int blocksCount = 16;
        std::array<ui8, blocksCount> encryptedBlockMask
            = {{0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0}};

        auto testClient = std::make_shared<TTestService>();
        auto testEncryptor = std::make_shared<TTestEncryptor>();

        auto encryptionClient = CreateEncryptionClient(
            testClient,
            logging,
            testEncryptor,
            GetDefaultEncryption());

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);

                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetBlockSize(blockSize);
                response.MutableVolume()->SetStorageMediaKind(mediaKind);
                return MakeFuture(std::move(response));
            };

        testClient->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                auto& sglist = guard.Get();

                TString bitmaskStr(request->GetBlocksCount() / 8 + 1, 0);
                for (size_t i = 0; i < request->GetBlocksCount(); ++i) {
                    if (!encryptedBlockMask[i]) {
                        size_t byte = i / 8;
                        size_t bit = i % 8;
                        bitmaskStr[byte] = (bitmaskStr[byte] | (1 << bit));
                    }

                    char value = 'a' + i;

                    if (encryptedBlockMask[i]) {
                        TString buf(blockSize, value);
                        TBlockDataRef bufRef(buf.data(), buf.size());
                        auto err = testEncryptor->Encrypt(bufRef, sglist[i], i);
                        UNIT_ASSERT_EQUAL_C(S_OK, err.GetCode(), err);
                    } else {
                        memset(
                            const_cast<char*>(sglist[i].Data()),
                            value,
                            blockSize);
                    }
                }

                NProto::TReadBlocksLocalResponse response;
                response.SetUnencryptedBlockMask(bitmaskStr);
                return MakeFuture(std::move(response));
            };

        auto mountResponse = MountVolume(*encryptionClient);
        UNIT_ASSERT(!HasError(mountResponse));

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(blocks, blocksCount, TString(blockSize, 0));
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetStartIndex(0);
        request->SetBlocksCount(blocksCount);
        request->BlockSize = blockSize;
        request->Sglist = TGuardedSgList(sglist);

        auto future = encryptionClient->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        for (size_t i = 0; i < sglist.size(); ++i) {
            if (IsDiskRegistryMediaKind(mediaKind) && !encryptedBlockMask[i]) {
                UNIT_ASSERT(BlockFilledByValue(sglist[i], 0));
                continue;
            }

            UNIT_ASSERT(BlockFilledByValue(sglist[i], 'a' + i));
        }
    }

#define BLOCKSTORE_IMPLEMENT_TEST(mediaKind, name, ...)                        \
    Y_UNIT_TEST(ShouldDecryptOverlayBlocksInReadBlocksLocalFor##name)          \
    {                                                                          \
        DoDecryptOverlayBlocksInReadBlocksLocal(mediaKind);                    \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_TEST

    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD, SSD)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_HYBRID, Hybrid)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_HDD, HDD)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED, SSDNonreplicated)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2, SSDMirror2)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3, SSDMirror3)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL, SSDLocal)

#undef BLOCKSTORE_IMPLEMENT_TEST

    Y_UNIT_TEST(SnapshotEncryptionClientShouldClearUnencryptedBlocksInReadBlocksResponse)
    {
        auto logging = CreateLoggingService("console");
        TString bitmaskStr;

        auto testClient = std::make_shared<TTestService>();

        testClient->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                NProto::TReadBlocksResponse response;
                ResizeIOVector(
                    *response.MutableBlocks(),
                    request->GetBlocksCount(),
                    DefaultBlockSize);
                response.SetUnencryptedBlockMask(bitmaskStr);
                return MakeFuture(response);
            };

        auto encryptionClient = CreateSnapshotEncryptionClient(
            testClient,
            logging,
            GetDefaultEncryption());

        auto ctx = MakeIntrusive<TCallContext>();
        auto request = std::make_shared<NProto::TReadBlocksRequest>();
        request->SetBlocksCount(4 * 8);

        bitmaskStr = TString(4, 0xFF);
        bitmaskStr[1] = 0x00;

        auto future = encryptionClient->ReadBlocks(ctx, request);
        const auto& response = future.GetValue(TDuration::Seconds(5));
        const auto& buffers = response.GetBlocks().GetBuffers();
        UNIT_ASSERT(buffers.size() == (int)request->GetBlocksCount());

        for (int i = 0; i < buffers.size(); ++i) {
            const auto& buffer = buffers[i];
            auto byte = i / 8;

            if (byte == 1) {
                UNIT_ASSERT(buffer.size() == DefaultBlockSize);
            } else {
                UNIT_ASSERT(buffer.empty());
            }
        }
    }

    Y_UNIT_TEST(SnapshotEncryptionClientShouldClearUnencryptedBlocksInReadBlocksLocalResponse)
    {
        auto logging = CreateLoggingService("console");
        TString bitmaskStr;

        auto testClient = std::make_shared<TTestService>();

        testClient->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                NProto::TReadBlocksLocalResponse response;
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                const auto& sglist = guard.Get();

                for (const auto& buffer: sglist) {
                    memset(const_cast<char*>(buffer.Data()), 'x', buffer.Size());
                }

                response.SetUnencryptedBlockMask(bitmaskStr);
                return MakeFuture(response);
            };

        auto encryptionClient = CreateSnapshotEncryptionClient(
            testClient,
            logging,
            GetDefaultEncryption());

        auto blocksCount = 4 * 8;

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString(DefaultBlockSize, 'a'));

        auto ctx = MakeIntrusive<TCallContext>();
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetBlocksCount(blocksCount);
        request->BlockSize = DefaultBlockSize;
        request->Sglist = TGuardedSgList(sglist);

        bitmaskStr = TString(4, 0xFF);
        bitmaskStr[1] = 0x00;

        auto future = encryptionClient->ReadBlocksLocal(ctx, request);
        future.GetValue(TDuration::Seconds(5));

        TString zeroBlock(DefaultBlockSize, 0);
        TString fillBlock(DefaultBlockSize, 'x');

        for (size_t i = 0; i < blocks.size(); ++i) {
            const auto& block = blocks[i];
            auto byte = i / 8;

            if (byte == 1) {
                UNIT_ASSERT_VALUES_EQUAL(fillBlock, block);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(zeroBlock, block);
            }
        }
    }

    Y_UNIT_TEST(SnapshotEncryptionClientShouldDenyZeroBlocksRequests)
    {
        auto logging = CreateLoggingService("console");
        auto encryptionClient = CreateSnapshotEncryptionClient(
            std::make_shared<TTestService>(),
            logging,
            GetDefaultEncryption());

        auto future = encryptionClient->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TZeroBlocksRequest>());

        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(response)
            && response.GetError().GetCode() == E_NOT_IMPLEMENTED);
    }

    Y_UNIT_TEST(ShouldHandleRequestsAfterDestroyClient)
    {
        auto logging = CreateLoggingService("console");
        ui32 blocksCount = 42;

        auto testClient = std::make_shared<TTestService>();

        auto encryptionClient = CreateEncryptionClient(
            testClient,
            logging,
            std::make_shared<TTestEncryptor>(),
            GetDefaultEncryption());

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest>) {
                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetBlockSize(DefaultBlockSize);
                return MakeFuture(response);
            };

        auto response = MountVolume(*encryptionClient);
        UNIT_ASSERT(!HasError(response));

        auto trigger = NewPromise<void>();

        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest>) {
                return trigger.GetFuture().Apply([] (const auto&) {
                    return NProto::TMountVolumeResponse();
                });
            };

        testClient->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest>) {
                return trigger.GetFuture().Apply([] (const auto&) {
                    return NProto::TReadBlocksResponse();
                });
            };

        testClient->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest>) {
                return trigger.GetFuture().Apply([] (const auto&) {
                    return NProto::TWriteBlocksResponse();
                });
            };

        testClient->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest>) {
                return trigger.GetFuture().Apply([] (const auto&) {
                    return NProto::TReadBlocksLocalResponse();
                });
            };

        testClient->WriteBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksLocalRequest>) {
                return trigger.GetFuture().Apply([] (const auto&) {
                    return NProto::TWriteBlocksLocalResponse();
                });
            };

        testClient->ZeroBlocksHandler =
            [&] (std::shared_ptr<NProto::TZeroBlocksRequest>) {
                return trigger.GetFuture().Apply([] (const auto&) {
                    return NProto::TZeroBlocksResponse();
                });
            };

        auto readRequest = std::make_shared<NProto::TReadBlocksRequest>();
        readRequest->SetBlocksCount(blocksCount);

        auto writeRequest = std::make_shared<NProto::TWriteBlocksRequest>();
        ResizeIOVector(*writeRequest->MutableBlocks(), blocksCount, DefaultBlockSize);

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString(DefaultBlockSize, 'x'));

        auto localReadRequest = std::make_shared<NProto::TReadBlocksLocalRequest>();
        localReadRequest->SetBlocksCount(blocksCount);
        localReadRequest->BlockSize = DefaultBlockSize;
        localReadRequest->Sglist = TGuardedSgList(sglist);

        auto localWriteRequest = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        localWriteRequest->BlocksCount = blocksCount;
        localWriteRequest->BlockSize = DefaultBlockSize;
        localWriteRequest->Sglist = TGuardedSgList(sglist);

        auto zeroRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        zeroRequest->SetBlocksCount(blocksCount);

        {
            auto mountFuture = encryptionClient->MountVolume(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TMountVolumeRequest>());
            UNIT_ASSERT(!mountFuture.HasValue());

            auto readFuture = encryptionClient->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                readRequest);
            UNIT_ASSERT(!readFuture.HasValue());

            auto writeFuture = encryptionClient->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                writeRequest);
            UNIT_ASSERT(!writeFuture.HasValue());

            auto localReadFuture = encryptionClient->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                localReadRequest);
            UNIT_ASSERT(!localReadFuture.HasValue());

            auto localWriteFuture = encryptionClient->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                localWriteRequest);
            UNIT_ASSERT(!localWriteFuture.HasValue());

            auto zeroFuture = encryptionClient->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                zeroRequest);
            UNIT_ASSERT(!zeroFuture.HasValue());

            encryptionClient.reset();
            trigger.SetValue();

            auto mountResponse = mountFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, mountResponse.GetError().GetCode());

            auto readResponse = readFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, readResponse.GetError().GetCode());

            auto writeResponse = writeFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(writeResponse), writeResponse);

            auto localReadResponse = localReadFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, localReadResponse.GetError().GetCode());

            auto localWriteResponse = localWriteFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(localWriteResponse), localWriteResponse);

            auto zeroResponse = zeroFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(zeroResponse), zeroResponse);
        }

        encryptionClient = CreateSnapshotEncryptionClient(
            testClient,
            logging,
            GetDefaultEncryption());

        trigger = NewPromise<void>();

        {
            auto mountFuture = encryptionClient->MountVolume(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TMountVolumeRequest>());
            UNIT_ASSERT(!mountFuture.HasValue());

            auto readFuture = encryptionClient->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                readRequest);
            UNIT_ASSERT(!readFuture.HasValue());

            auto writeFuture = encryptionClient->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                writeRequest);
            UNIT_ASSERT(!writeFuture.HasValue());

            auto localReadFuture = encryptionClient->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                localReadRequest);
            UNIT_ASSERT(!localReadFuture.HasValue());

            auto localWriteFuture = encryptionClient->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                localWriteRequest);
            UNIT_ASSERT(!localWriteFuture.HasValue());

            auto zeroFuture = encryptionClient->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                zeroRequest);
            auto zeroResponse = zeroFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_IMPLEMENTED, zeroResponse.GetError().GetCode());

            encryptionClient.reset();
            trigger.SetValue();

            auto mountResponse = mountFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(mountResponse), mountResponse);

            auto readResponse = readFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(readResponse), readResponse);

            auto writeResponse = writeFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(writeResponse), writeResponse);

            auto localReadResponse = localReadFuture.GetValue(TDuration::Seconds(5));
            // localReadResponse is not a protobuf message based, so it cannot
            // be serialized and printed via Out<>
            const auto& baseResponse = static_cast<
                const NCloud::NBlockStore::NProto::TReadBlocksResponse&>(
                localReadResponse);
            UNIT_ASSERT_C(!HasError(localReadResponse), baseResponse);

            auto localWriteResponse = localWriteFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(localWriteResponse), localWriteResponse);
        }
    }

    Y_UNIT_TEST_F(ShouldCreateEncryptionClientDependingOnVolumeConfig, TFixture)
    {
        auto fillWithEncryption = [&] (auto& sglist, ui32 blocksCount) mutable {
            for (ui32 i = 0; i != blocksCount; ++i) {
                TString buf(BlockSize, 'a' + i);
                TBlockDataRef bufRef(buf.data(), buf.size());
                auto err = Encryptor->Encrypt(bufRef, sglist[i], i);
                UNIT_ASSERT_VALUES_EQUAL_C(S_OK, err.GetCode(), err);
            }
        };

        auto fillWithoutEncryption = [&] (auto& sglist, ui32 blocksCount) mutable {
            for (ui32 i = 0; i != blocksCount; ++i) {
                auto& block = sglist[i];
                memset(const_cast<char*>(block.Data()), 'a' + i, block.Size());
            }
        };

        auto testClient = std::make_shared<TTestService>();

        // Mount a volume encrypted with ENCRYPTION_AES_XTS without encryption
        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT(!request->HasEncryptionSpec());

                NProto::TEncryptionDesc encryption;
                encryption.SetMode(NProto::ENCRYPTION_AES_XTS);
                encryption.SetKeyHash("testKeyHash");

                NProto::TMountVolumeResponse response;
                *response.MutableVolume()->MutableEncryptionDesc() = encryption;

                return MakeFuture(std::move(response));
            };

        {
            auto encryptionClient =
                CreateVolumeEncryptionClient(testClient, KeyProvider, Logging);

            auto response = MountVolume(*encryptionClient);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,   // It's OK to mount an encrypted disk without an
                        // encryption key to make snapshot or fill from snapshot
                response.GetError().GetCode(),
                response.GetError());
        }

        // Mount an encrypted Volume
        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT(!request->HasEncryptionSpec());
                NProto::TMountVolumeResponse response;
                auto& volume = *response.MutableVolume();

                volume.SetBlockSize(BlockSize);

                NProto::TEncryptionDesc& desc = *volume.MutableEncryptionDesc();
                desc.SetMode(NProto::ENCRYPTION_AT_REST);
                desc.MutableEncryptionKey()->SetKekId(KekId);
                desc.MutableEncryptionKey()->SetEncryptedDEK(
                    Base64Encode(EncryptionKey));

                return MakeFuture(std::move(response));
            };

        // Let's try to read from the encrypted Volume

        testClient->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(0, request->GetStartIndex());

                NProto::TReadBlocksResponse response;

                auto sglist = ResizeIOVector(
                    *response.MutableBlocks(),
                    request->GetBlocksCount(),
                    BlockSize);

                fillWithEncryption(sglist, request->GetBlocksCount());

                return MakeFuture(std::move(response));
            };

        testClient->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                fillWithEncryption(guard.Get(), request->GetBlocksCount());

                return MakeFuture(NProto::TReadBlocksLocalResponse());
            };

        {
            auto encryptionClient =
                CreateVolumeEncryptionClient(testClient, KeyProvider, Logging);

            auto response = MountVolume(*encryptionClient);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError());

            constexpr ui32 blockCount = 7;

            // remote
            {
                auto request = std::make_shared<NProto::TReadBlocksRequest>();
                request->SetBlocksCount(blockCount);

                auto future = encryptionClient->ReadBlocks(
                    MakeIntrusive<TCallContext>(),
                    std::move(request));
                const auto& response = future.GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    response.GetError().GetCode(),
                    response.GetError());

                const auto& buffers = response.GetBlocks().GetBuffers();
                for (ui32 i = 0; i != blockCount; ++i) {
                    TBlockDataRef block(buffers[i].data(), buffers[i].size());
                    UNIT_ASSERT(BlockFilledByValue(block, 'a' + i));
                }
            }

            // local
            {
                TVector<TString> blocks;
                auto sglist =
                    ResizeBlocks(blocks, blockCount, TString(BlockSize, 0));
                auto request =
                    std::make_shared<NProto::TReadBlocksLocalRequest>();
                request->SetStartIndex(0);
                request->SetBlocksCount(blockCount);
                request->BlockSize = BlockSize;
                request->Sglist = TGuardedSgList(sglist);

                auto future = encryptionClient->ReadBlocksLocal(
                    MakeIntrusive<TCallContext>(),
                    std::move(request));
                auto response = future.GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    response.GetError().GetCode(),
                    response.GetError());

                for (size_t i = 0; i < sglist.size(); ++i) {
                    UNIT_ASSERT(BlockFilledByValue(sglist[i], 'a' + i));
                }
            }
        }

        // Mount an unencrypted Volume
        testClient->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT(!request->HasEncryptionSpec());
                NProto::TMountVolumeResponse response;
                auto& volume = *response.MutableVolume();
                volume.SetBlockSize(BlockSize);
                return MakeFuture(std::move(response));
            };

        // Let's try to read from the unencrypted Volume

        testClient->ReadBlocksHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(0, request->GetStartIndex());

                NProto::TReadBlocksResponse response;

                auto sglist = ResizeIOVector(
                    *response.MutableBlocks(),
                    request->GetBlocksCount(),
                    BlockSize);

                fillWithoutEncryption(sglist, request->GetBlocksCount());

                return MakeFuture(std::move(response));
            };

        testClient->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);

                fillWithoutEncryption(guard.Get(), request->GetBlocksCount());

                return MakeFuture(NProto::TReadBlocksLocalResponse());
            };

        {
            auto encryptionClient =
                CreateVolumeEncryptionClient(testClient, KeyProvider, Logging);

            auto response = MountVolume(*encryptionClient);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError());

            constexpr ui32 blockCount = 7;

            // remote
            {
                auto request = std::make_shared<NProto::TReadBlocksRequest>();
                request->SetBlocksCount(blockCount);

                auto future = encryptionClient->ReadBlocks(
                    MakeIntrusive<TCallContext>(),
                    std::move(request));
                const auto& response = future.GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    response.GetError().GetCode(),
                    response.GetError());

                const auto& buffers = response.GetBlocks().GetBuffers();
                for (ui32 i = 0; i != blockCount; ++i) {
                    TBlockDataRef block(buffers[i].data(), buffers[i].size());
                    UNIT_ASSERT(BlockFilledByValue(block, 'a' + i));
                }
            }

            // local
            {
                TVector<TString> blocks;
                auto sglist =
                    ResizeBlocks(blocks, blockCount, TString(BlockSize, 0));
                auto request =
                    std::make_shared<NProto::TReadBlocksLocalRequest>();
                request->SetStartIndex(0);
                request->SetBlocksCount(blockCount);
                request->BlockSize = BlockSize;
                request->Sglist = TGuardedSgList(sglist);

                auto future = encryptionClient->ReadBlocksLocal(
                    MakeIntrusive<TCallContext>(),
                    std::move(request));
                auto response = future.GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    response.GetError().GetCode(),
                    response.GetError());

                for (size_t i = 0; i < sglist.size(); ++i) {
                    UNIT_ASSERT(BlockFilledByValue(sglist[i], 'a' + i));
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore
