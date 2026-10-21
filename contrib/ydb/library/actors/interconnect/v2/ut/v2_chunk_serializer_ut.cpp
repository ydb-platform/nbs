#include <contrib/ydb/library/actors/interconnect/v2/v2_chunk_serializer.h>

#include <contrib/ydb/library/actors/core/event_pb.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/protos/unittests.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/restricted/abseil-cpp-tstring/y_absl/strings/cord.h>
#include <contrib/restricted/abseil-cpp-tstring/y_absl/strings/cord_test_helpers.h>

#include <util/random/random.h>

using namespace NActors;

namespace {

    enum {
        EvMessageWithPayload = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

    struct TEvMessageWithPayload : TEventPB<TEvMessageWithPayload, TMessageWithPayload, EvMessageWithPayload> {
    };

    struct TMockEvent: public IEventBase {
        TBigMessage* msg;
        bool SerializeToArcadiaStream(TChunkSerializer* chunker) const override {
            return msg->SerializeToZeroCopyStream(chunker);
        }
        bool IsSerializable() const override {
            return true;
        }
        TString ToStringHeader() const override {
            return TString();
        }
        ui32 Type() const override {
            return 0;
        };
    };

    TRope MakeStringRope(const TString& message) {
        return message ? TRope(message) : TRope();
    }

    TString MakeString(size_t len) {
        TString res;
        for (size_t i = 0; i < len; ++i) {
            res += RandomNumber<char>();
        }
        return res;
    }

    TString SerializeWithChunker(const IEventBase& event, size_t feedBufSize) {
        TString out;
        TCoroutineChunkSerializerV2 chunker;
        chunker.SetSerializingEvent(&event, /*withCord=*/false);
        while (!chunker.IsComplete()) {
            TString buffer(feedBufSize, '\0');
            for (const auto& chunk : chunker.FeedBuf(buffer.begin(), buffer.size())) {
                out.append(chunk.Buf, chunk.Size);
            }
        }
        UNIT_ASSERT(chunker.IsSuccessfull());
        return out;
    }

    TString SerializeWithAllocChunker(const IEventBase& event) {
        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(event.SerializeToArcadiaStream(serializer.Get()));
        return serializer->Release(event.CreateSerializationInfo())->GetString();
    }

}

Y_UNIT_TEST_SUITE(TCoroutineChunkSerializerV2) {
    Y_UNIT_TEST(Coroutine) {
        TString strA(507, 'a');
        TString strB(814, 'b');
        TString strC(198, 'c');

        TBigMessage bm;

        TSimple* simple0 = bm.AddSimples();
        simple0->SetStr1(strA);
        simple0->SetStr2(strB);
        simple0->SetNumber1(213431324);

        TSimple* simple1 = bm.AddSimples();
        simple1->SetStr1(strC);
        simple1->SetStr2(strA);
        simple1->SetNumber1(21039313);

        bm.AddManyStr(strA);
        bm.AddManyStr(strC);
        bm.AddManyStr(strB);

        bm.SetOneMoreStr(strB);
        bm.SetYANumber(394143);

        TString bmSerialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD bm.SerializeToString(&bmSerialized);
        UNIT_ASSERT_UNEQUAL(bmSerialized.size(), 0);

        TCoroutineChunkSerializerV2 chunker;
        for (int i = 0; i < 4; ++i) {
            TMockEvent event;
            event.msg = &bm;
            chunker.SetSerializingEvent(&event, false);
            char buf1[87];
            TString bmChunkedSerialized;
            while (!chunker.IsComplete()) {
                auto range = chunker.FeedBuf(&buf1[0], sizeof(buf1));
                for (const auto& chunk : range) {
                    bmChunkedSerialized.append(chunk.Buf, chunk.Size);
                }
            }
            UNIT_ASSERT_EQUAL(bmSerialized, bmChunkedSerialized);
        }
    }

    Y_UNIT_TEST(PayloadMatchesAllocChunkSerializer) {
        for (size_t size1 : {0, 1, 100, 4095, 4096, 9000}) {
            for (size_t size2 : {0, 111, 5000}) {
                TEvMessageWithPayload msg;
                msg.Record.SetMeta("hello, world!");
                msg.Record.AddPayloadId(msg.AddPayload(MakeStringRope(MakeString(size1))));
                msg.Record.AddPayloadId(msg.AddPayload(MakeStringRope(MakeString(size2))));
                msg.Record.AddSomeData(MakeString((size1 + size2) % 50 + 11));

                const TString expected = SerializeWithAllocChunker(msg);
                UNIT_ASSERT_VALUES_EQUAL(expected.size(), msg.CalculateSerializedSize());
                UNIT_ASSERT_VALUES_EQUAL_C(SerializeWithChunker(msg, 4096), expected,
                    "size1# " << size1 << " size2# " << size2);
            }
        }
    }

    Y_UNIT_TEST(ChunkSizesAroundSlopBoundary) {
        TEvMessageWithPayload msg;
        msg.Record.SetMeta("hello, world!");
        msg.Record.AddPayloadId(msg.AddPayload(MakeStringRope(MakeString(256))));
        msg.Record.AddSomeData(MakeString(128));

        const TString expected = SerializeWithAllocChunker(msg);
        UNIT_ASSERT_VALUES_EQUAL(expected.size(), msg.CalculateSerializedSize());

        for (size_t chunkSize = 1; chunkSize <= 32; ++chunkSize) {
            UNIT_ASSERT_VALUES_EQUAL_C(SerializeWithChunker(msg, chunkSize), expected, "chunkSize# " << chunkSize);
        }
    }

    Y_UNIT_TEST(AbortStopsSerialization) {
        TEvMessageWithPayload msg;
        msg.Record.AddPayloadId(msg.AddPayload(MakeStringRope(MakeString(8192))));

        TCoroutineChunkSerializerV2 chunker;
        chunker.SetSerializingEvent(&msg, false);
        char buf[64];
        chunker.FeedBuf(buf, sizeof(buf));
        UNIT_ASSERT(!chunker.IsComplete());
        chunker.Abort();
        UNIT_ASSERT(chunker.IsComplete());
        UNIT_ASSERT(!chunker.IsSuccessfull());

        const TString expected = SerializeWithAllocChunker(msg);
        UNIT_ASSERT_VALUES_EQUAL(SerializeWithChunker(msg, 4096), expected);
    }
}

Y_UNIT_TEST_SUITE(TCoroutineChunkSerializerV2Cord) {
    // Exercises WriteCord directly — open-source protoc does not emit ctype=CORD fields, so generated
    // protobuf never calls WriteCord on its own.
    struct TCordEvent: public IEventBase {
        y_absl::Cord CordData;

        bool SerializeToArcadiaStream(TChunkSerializer* chunker) const override {
            return chunker->WriteCord(CordData);
        }
        bool IsSerializable() const override {
            return true;
        }
        TString ToStringHeader() const override {
            return TString();
        }
        ui32 Type() const override {
            return 0;
        }
        ui32 CalculateSerializedSize() const override {
            return CordData.size();
        }
    };

    TString SerializeCordEvent(const y_absl::Cord& cord, bool withCord, size_t feedBufSize,
            std::vector<y_absl::Cord>* retainedCords = nullptr)
    {
        TCordEvent event;
        event.CordData = cord;

        TCoroutineChunkSerializerV2 chunker;
        chunker.SetSerializingEvent(&event, withCord);

        TString out;
        std::vector<char> buf(Max<size_t>(feedBufSize, 1));
        while (!chunker.IsComplete()) {
            // Next FeedBuf aborts unless the caller drained GetCords() after the previous one.
            UNIT_ASSERT(chunker.GetCords().empty());
            auto range = chunker.FeedBuf(buf.data(), buf.size());
            for (const auto& chunk : range) {
                out.append(chunk.Buf, chunk.Size);
            }
            // WriteCord only pushes into GetCords() after all chunks are written; intermediate FeedBuf
            // calls that yield mid-Cord leave GetCords empty (event still owns the Cord).
            auto& cords = chunker.GetCords();
            if (retainedCords) {
                retainedCords->insert(retainedCords->end(), cords.begin(), cords.end());
            }
            cords.clear();
        }
        UNIT_ASSERT(chunker.GetCords().empty());
        return out;
    }

    Y_UNIT_TEST(WriteCordAliasAndCopyParity) {
        const std::vector<y_absl::Cord> cases = {
            y_absl::Cord(),
            y_absl::Cord("single-chunk-cord"),
            y_absl::Cord(TString(4096, 'x')),
            y_absl::MakeFragmentedCord({"A ", "fragmented ", "Cord", TString(2000, 'z')}),
        };
        const std::vector<size_t> feedSizes = {1, 2, 3, 7, 16, 64, 512, 8192};

        for (const auto& cord : cases) {
            const TString expected(cord);
            for (size_t feedSize : feedSizes) {
                const TString aliased = SerializeCordEvent(cord, /*withCord=*/true, feedSize);
                const TString copied = SerializeCordEvent(cord, /*withCord=*/false, feedSize);
                UNIT_ASSERT_VALUES_EQUAL(aliased, expected);
                UNIT_ASSERT_VALUES_EQUAL(copied, expected);
                UNIT_ASSERT_VALUES_EQUAL(aliased, copied);
            }
        }
    }

    Y_UNIT_TEST(WriteCordRetainsOwnershipWhenAliasing) {
        // Build a cord whose backing store would be freed if we only held the original Cord object.
        TString backing(8192, 'q');
        for (size_t i = 0; i < backing.size(); ++i) {
            backing[i] = static_cast<char>('a' + (i % 26));
        }
        const TString expected = backing;

        y_absl::Cord cord(backing);
        backing.clear(); // drop the local string; cord still refs the data

        std::vector<y_absl::Cord> retained;
        TCordEvent event;
        event.CordData = std::move(cord); // event holds the only Cord until WriteCord copies it into Cords

        TCoroutineChunkSerializerV2 chunker;
        chunker.SetSerializingEvent(&event, /*withCord=*/true);

        char buf[128];
        TString out;
        std::vector<std::pair<const char*, size_t>> aliasedSpans;
        while (!chunker.IsComplete()) {
            auto range = chunker.FeedBuf(buf, sizeof(buf));
            for (const auto& chunk : range) {
                // Spans that point outside the scratch buffer are aliases into Cord memory.
                if (chunk.Buf < buf || chunk.Buf >= buf + sizeof(buf)) {
                    aliasedSpans.emplace_back(chunk.Buf, chunk.Size);
                }
                out.append(chunk.Buf, chunk.Size);
            }
            auto& cords = chunker.GetCords();
            retained.insert(retained.end(), cords.begin(), cords.end());
            cords.clear();
        }

        // Drop the event's Cord; retained copies from GetCords() must keep aliased bytes alive.
        event.CordData.Clear();

        volatile ui64 acc = 0;
        for (const auto& [ptr, size] : aliasedSpans) {
            for (size_t i = 0; i < size; ++i) {
                acc += static_cast<ui8>(ptr[i]);
            }
        }
        Y_UNUSED(acc);

        UNIT_ASSERT_VALUES_EQUAL(out, expected);
        UNIT_ASSERT(!retained.empty() || expected.empty());
    }

    Y_UNIT_TEST(WriteCordDrainContractAcrossFeedBuf) {
        // A large fragmented cord forces multiple FeedBuf calls; each must leave GetCords drained
        // before the next call (ICv2 does this after every FeedBuf).
        y_absl::Cord cord = y_absl::MakeFragmentedCord({
            TString(3000, 'a'),
            TString(3000, 'b'),
            TString(3000, 'c'),
        });
        const TString expected(cord);

        TCordEvent event;
        event.CordData = cord;

        TCoroutineChunkSerializerV2 chunker;
        chunker.SetSerializingEvent(&event, /*withCord=*/true);

        char buf[256];
        TString out;
        size_t feedCount = 0;
        bool sawRetainedCord = false;
        while (!chunker.IsComplete()) {
            UNIT_ASSERT(chunker.GetCords().empty());
            auto range = chunker.FeedBuf(buf, sizeof(buf));
            ++feedCount;
            for (const auto& chunk : range) {
                out.append(chunk.Buf, chunk.Size);
            }
            if (!chunker.GetCords().empty()) {
                sawRetainedCord = true;
            }
            chunker.GetCords().clear();
        }
        UNIT_ASSERT_GT(feedCount, 1);
        UNIT_ASSERT(sawRetainedCord);
        UNIT_ASSERT_VALUES_EQUAL(out, expected);
    }
}
