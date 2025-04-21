#include "session_sequencer.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/random/random.h>

#include <functional>
#include <optional>
#include <tuple>

namespace NCloud::NFileStore::NFuse {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    ui64 Handle = 0;
    ui64 Offset = 0;
    ui64 Length = 0;

    ui64 Id = 0;
    bool IsRead = false;
    std::function<void()> Done;

    bool operator==(const TRequest& r) const
    {
        return std::tie(Handle, Offset, Length, IsRead) ==
            std::tie(r.Handle, r.Offset, r.Length, r.IsRead);
    }

    bool operator<(const TRequest& r) const
    {
        return std::tie(Handle, Offset, Length, IsRead) <
            std::tie(r.Handle, r.Offset, r.Length, r.IsRead);
    }
};

#define R(handle, offset, length) \
    TRequest { \
        .Handle = handle, \
        .Offset = offset, \
        .Length = length, \
        .IsRead = true,   \
        .Done = {} \
    }

#define W(handle, offset, length) \
    TRequest { \
        .Handle = handle, \
        .Offset = offset, \
        .Length = length, \
        .IsRead = false, \
        .Done = {} \
    }

IOutputStream& operator<<(IOutputStream& out, const TRequest& r) {
    if (r.IsRead) {
        out << "R";
    } else {
        out << "W";
    }
    out << "("
        << r.Handle << ", "
        << r.Offset << ", "
        << r.Length
        << ")";
    return out;
}

TString PrintRequests(const TVector<TRequest>& requests)
{
    TStringBuilder out;

    for (size_t i = 0; i != requests.size(); i++) {
        out << requests[i];

        if (i != requests.size() - 1) {
            out << ", ";
        }
    }

    return out;
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    ILoggingServicePtr Logging;
    TLog Log;

    std::shared_ptr<TFileStoreTest> Session;
    TSessionSequencerPtr SessionSequencer;

    TCallContextPtr CallContext;

    ui64 NextRequestId = 0;
    TVector<TRequest> InFlightRequests;

    // map from handle
    THashMap<ui64, TString> ExpectedData;

    TVector<TFuture<NProto::TReadDataResponse>> ReadDataFutures;
    TVector<TFuture<NProto::TWriteDataResponse>> WriteDataFutures;

    TBootstrap()
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("SESSION_SEQUENCER");

        Session = std::make_shared<TFileStoreTest>();

        Session->ReadDataHandler = [&] (auto, auto protoRequest) {
            using TResponse = NProto::TReadDataResponse;

            struct TState {
                TPromise<TResponse> Promise = NewPromise<TResponse>();
            };

            auto state = std::make_shared<TState>();

            const auto id = ++NextRequestId;

            const auto handle = protoRequest->GetHandle();
            const auto offset = protoRequest->GetOffset();
            const auto length = protoRequest->GetLength();

            InFlightRequests.push_back(TRequest {
                .Handle = handle,
                .Offset = offset,
                .Length = length,

                .Id = id,
                .IsRead = true,

                .Done = [&, state, id, handle, offset, length] () {
                    EraseIf(
                        InFlightRequests,
                        [id] (const auto& r) { return r.Id == id; });

                    Y_ABORT_UNLESS(ExpectedData.contains(handle));
                    Y_ABORT_UNLESS(length <= ExpectedData[handle].length());

                    TResponse response;
                    auto data = TStringBuf(ExpectedData[handle]);
                    auto substr = data.SubString(offset, length);
                    response.SetBuffer(TString(substr));

                    state->Promise.SetValue(response);
                }
            });

            return state->Promise.GetFuture();
        };

        Session->WriteDataHandler = [&] (auto, auto protoRequest) {
            using TResponse = NProto::TWriteDataResponse;

            struct TState {
                TPromise<TResponse> Promise = NewPromise<TResponse>();
            };

            auto state = std::make_shared<TState>();

            const auto id = ++NextRequestId;

            InFlightRequests.push_back(TRequest {
                .Handle = protoRequest->GetHandle(),
                .Offset = protoRequest->GetOffset(),
                .Length = protoRequest->GetBuffer().length(),

                .Id = id,
                .IsRead = false,

                .Done = [&, state, id] () {
                    EraseIf(
                        InFlightRequests,
                        [id] (const auto& r) { return r.Id == id; });

                    state->Promise.SetValue({});
                }
            });

            return state->Promise.GetFuture();
        };

        SessionSequencer = CreateSessionSequencer(Session);

        CallContext = MakeIntrusive<TCallContext>();
    }

    ~TBootstrap() = default;

    void ReadData(ui64 handle, ui64 offset, ui64 length)
    {
        auto request = std::make_shared<NProto::TReadDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(length);

        auto future = SessionSequencer->ReadData(
            CallContext,
            std::move(request));

        future.Subscribe([&, handle, offset, length] (auto future) {
            Y_ABORT_UNLESS(ExpectedData.contains(handle));
            Y_ABORT_UNLESS(length <= ExpectedData[handle].length());

            auto data = TStringBuf(ExpectedData[handle]);
            auto substr = data.SubString(offset, length);
            auto response = future.GetValue();
            UNIT_ASSERT_VALUES_EQUAL(substr, response.GetBuffer());
        });

        ReadDataFutures.push_back(future);
    }

    void WriteData(ui64 handle, ui64 offset, TString buffer)
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetBuffer(buffer);

        auto future = SessionSequencer->WriteData(
            CallContext,
            std::move(request));

        future.Subscribe([&, handle, offset, buffer] (auto) {
            auto* data = &ExpectedData[handle];
            // append zeroes if needed
            auto newSize = Max(data->size(), offset + buffer.size());
            data->resize(newSize, 0);
            data->replace(offset, buffer.size(), buffer);
        });

        WriteDataFutures.push_back(future);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSessionSequencerTest)
{
    void TestShouldReadAndWrite(
        TVector<TVector<TRequest>> expectedLevels)
    {
        TBootstrap b;

        const auto maxLength = 100;
        b.ExpectedData[0] = TString(maxLength, 0);
        b.ExpectedData[1] = TString(maxLength, 0);
        b.ExpectedData[2] = TString(maxLength, 0);

        for (const auto& expectedRequests: expectedLevels) {
            for (const auto& request: expectedRequests) {
                if (request.IsRead) {
                    b.ReadData(request.Handle, request.Offset, request.Length);
                } else {
                    TString buffer(request.Length, 'a');
                    b.WriteData(request.Handle, request.Offset, buffer);
                }
            }
        }

        // print levels
        for (size_t i = 0; i != expectedLevels.size(); i++) {
            const auto& Log = b.Log;
            STORAGE_INFO(
                "Level " << i << ": " << PrintRequests(expectedLevels[i]));
        }

        for (size_t i = 0; i != expectedLevels.size(); i++) {
            auto& expectedRequests = expectedLevels[i];

            TVector<TRequest> actualRequests;
            for (auto& request: b.InFlightRequests) {
                actualRequests.push_back(request);
            }

            Sort(expectedRequests);

            auto sortedActualRequests = actualRequests;
            Sort(sortedActualRequests);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedRequests,
                sortedActualRequests,
                TStringBuilder() << "while validating level " << i
            );

            for (const auto& request: actualRequests) {
                request.Done();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(0, b.InFlightRequests.size());

        for (const auto& future: b.ReadDataFutures) {
            UNIT_ASSERT(future.HasValue());
        }
        for (const auto& future: b.WriteDataFutures) {
            UNIT_ASSERT(future.HasValue());
        }
    }

    Y_UNIT_TEST(ShouldReadAndWrite)
    {
        TestShouldReadAndWrite({
            {
                W(1, 10, 6), W(1, 20, 7), W(1, 30, 8), R(1, 0, 10),
                R(1, 45, 15), R(1, 60, 25), R(1, 75, 50), W(2, 10, 6),
                W(2, 20, 7)
            },
            {
                W(1, 10, 6), W(1, 20, 7), W(1, 45, 15), W(2, 10, 6),
                R(2, 20, 7)
            },
            {
                W(1, 10, 6), W(1, 20, 7), R(2, 10, 6), R(2, 10, 6),
                W(2, 20, 7)
            },
            {
                W(2, 20, 7), R(1, 20, 7)
            }
        });
        TestShouldReadAndWrite({
            {
                W(0, 4, 14), R(1, 4, 8), R(1, 6, 20), R(1, 6, 11), R(1, 0, 9),
                R(2, 3, 7), R(2, 0, 18), R(2, 3, 2), R(2, 0, 11), R(1, 4, 5),
                R(2, 3, 4), W(0, 0, 1), W(0, 1, 0), R(1, 3, 20), R(2, 1, 6),
                R(1, 7, 19)
            },
            {
                R(0, 10, 5), R(0, 4, 18), R(0, 5, 18), W(1, 2, 1), W(2, 8, 7),
                R(0, 6, 8), W(0, 0, 1), R(0, 5, 8)
            },
            {
                W(0, 10, 8), W(2, 0, 11), W(1, 2, 18)
            },
            {
                W(0, 2, 9), W(2, 7, 3)
            },
            {
                W(0, 2, 17), W(2, 3, 12)
            },
            {
                R(0, 0, 15), W(2, 4, 13)
            }
        });
        TestShouldReadAndWrite({
            {
                R(1, 2, 17), R(2, 4, 21), R(2, 4, 3), R(2, 5, 5), W(0, 8, 10),
                R(1, 5, 20), W(0, 3, 3), R(2, 3, 17), R(2, 10, 1), R(1, 5, 17),
                R(2, 4, 7), R(2, 8, 7), R(0, 1, 0), R(2, 4, 11), R(2, 1, 15),
                R(2, 2, 7), R(1, 8, 15), R(1, 8, 19), R(2, 5, 3), R(2, 2, 15),
                W(0, 7, 0), R(1, 6, 15), R(1, 1, 4), R(2, 8, 3)
            },
            {
                W(1, 8, 17), W(2, 6, 5), R(0, 6, 12), R(0, 2, 7), R(0, 0, 7),
                R(0, 5, 3), R(0, 10, 1), R(0, 7, 16), R(0, 8, 18), R(0, 10, 16),
                W(2, 2, 4), R(0, 7, 4), R(0, 9, 2)
            },
            {
                W(2, 3, 17), W(0, 8, 19), W(0, 2, 2), W(0, 4, 3), W(1, 4, 10)
            },
            {
                W(2, 9, 16), W(1, 7, 5), W(0, 7, 16)
            },
            {
                W(1, 5, 9), W(0, 1, 15), W(2, 4, 12)
            },
            {
                W(1, 7, 17), W(0, 7, 11), W(2, 8, 20)
            },
            {
                W(1, 9, 20), W(0, 1, 21), W(2, 7, 20)
            },
            {
                W(0, 7, 3)
            }
        });
    }

    Y_UNIT_TEST(ShouldReadAndWriteRandomized)
    {
        TBootstrap b;

        const auto maxLength = 100;
        b.ExpectedData[0] = TString(maxLength, 0);
        b.ExpectedData[1] = TString(maxLength, 0);
        b.ExpectedData[2] = TString(maxLength, 0);

        auto requestsRemaining = 222;

        TVector<TRequest> requests;
        while (requestsRemaining--) {
            const auto handle = RandomNumber<ui64>(3);
            const auto offset = RandomNumber<ui64>(11);
            const auto length = RandomNumber<ui64>(22);

            if (RandomNumber<ui64>(2)) {
                requests.push_back(R(handle, offset, length));
            } else {
                requests.push_back(W(handle, offset, length));
            }
        }

        {
            const auto& Log = b.Log;
            STORAGE_INFO("Requests: " << PrintRequests(requests));
        }

        for (const auto& request: requests) {
            if (request.IsRead) {
                b.ReadData(request.Handle, request.Offset, request.Length);
            } else {
                TString buffer(request.Length, 'a');
                b.WriteData(request.Handle, request.Offset, buffer);
            }
        }

        requestsRemaining = requests.size();
        while (requestsRemaining--) {
            UNIT_ASSERT_C(
                !b.InFlightRequests.empty(),
                PrintRequests(b.InFlightRequests));

            auto request = b.InFlightRequests.front();
            request.Done();

            for (size_t i = 0; i < b.InFlightRequests.size(); i++) {
                for (size_t j = i + 1; j < b.InFlightRequests.size(); j++) {
                    const auto& request = b.InFlightRequests[i];
                    const auto& otherRequest = b.InFlightRequests[j];

                    if (request.Handle != otherRequest.Handle) {
                        continue;
                    }

                    if (request.IsRead && otherRequest.IsRead) {
                        continue;
                    }

                    const auto offset = Max(request.Offset, otherRequest.Offset);
                    const auto end = Min(
                        request.Offset + request.Length,
                        otherRequest.Offset + otherRequest.Length);

                    if (offset < end) {
                        UNIT_ASSERT_C(
                            false,
                            "Found overlapping requests: "
                                << PrintRequests(b.InFlightRequests));
                    }
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL_C(
            0,
            b.InFlightRequests.size(),
            PrintRequests(b.InFlightRequests));

        for (const auto& future: b.ReadDataFutures) {
            UNIT_ASSERT(future.HasValue());
        }
        for (const auto& future: b.WriteDataFutures) {
            UNIT_ASSERT(future.HasValue());
        }
    }
}

}   // namespace NCloud::NFileStore::NFuse
