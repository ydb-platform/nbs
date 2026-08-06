#include "dynamic_persistent_table.h"

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/tempdir.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>

#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCrashTestHeader
{
};

using TTable = TDynamicPersistentTable<TCrashTestHeader>;

struct TRecordHeader
{
    ui32 BodyCrc32 = 0;
};

constexpr TDuration CrashInterval = TDuration::Seconds(1);
constexpr ui64 CrashCyclesPerStrategy = 2;
constexpr ui64 TargetLiveRecords = 500;

enum class EMutationStrategy
{
    UpDown,
    Random,
};

TString LastSystemError()
{
    return TStringBuilder() << std::strerror(errno) << " (" << errno << ")";
}

TDynamicPersistentTableConfig MakeTableConfig()
{
    return {
        .MaxRecords = 4,
        .InitialDataAreaSize = 512,
        .MaxDataAreaStepSize = 512,
        .InitialDataMoveBufferSize = 128,
        .GapSpacePercentageCompactionThreshold = 10,
        .ShrinkLowMemoryOpThreshold = 1,
        .ShrinkMode = EDynamicPersistentTableShrinkMode::AnyOp,
    };
}

TTable CreateTable(const TString& fileName)
{
    return TTable(
        fileName,
        MakeTableConfig(),
        CreateFileMapMemoryLimiterStub());
}

TString MakeRecord(ui32 bodySize, ui64 salt)
{
    TString result(sizeof(TRecordHeader) + bodySize, '\0');
    char* data = result.begin();
    char* body = data + sizeof(TRecordHeader);

    for (ui32 i = 0; i < bodySize; ++i) {
        body[i] = static_cast<char>((salt + i) & 0xff);
    }

    TRecordHeader header;
    header.BodyCrc32 = Crc32c(body, bodySize);
    std::memcpy(data, &header, sizeof(header));

    return result;
}

bool ValidateRecord(TStringBuf record)
{
    if (record.size() < sizeof(TRecordHeader)) {
        return false;
    }

    TRecordHeader header;
    std::memcpy(&header, record.data(), sizeof(header));

    const ui64 bodySize = record.size() - sizeof(TRecordHeader);
    if (!bodySize) {
        return false;
    }

    const char* body = record.data() + sizeof(TRecordHeader);
    if (Crc32c(body, bodySize) != header.BodyCrc32) {
        return false;
    }

    return true;
}

TVector<ui64> ValidateAndCollectLiveRecords(TTable& table)
{
    TVector<ui64> liveRecords;
    THashSet<ui64> seenIndices;

    for (auto it = table.begin(); it != table.end(); ++it) {
        const ui64 index = it.GetIndex();
        UNIT_ASSERT_C(seenIndices.insert(index).second, index);

        const TStringBuf iteratorRecord = *it;
        UNIT_ASSERT_C(!iteratorRecord.empty(), index);
        UNIT_ASSERT_C(ValidateRecord(iteratorRecord), index);

        liveRecords.push_back(index);
    }

    UNIT_ASSERT_VALUES_EQUAL(liveRecords.size(), table.CountRecords());
    return liveRecords;
}

void ValidateTableFromFile(const TString& tablePath)
{
    auto table = CreateTable(tablePath);
    ValidateAndCollectLiveRecords(table);
}

enum class EChildFailure
{
    EmptyLiveRecords = 1,
    AllocRecordFailed,
    WriteRecordFailed,
    CommitRecordFailed,
    SelectedRecordInvalid,
    RewriteRecordFailed,
    RewrittenRecordInvalid,
    DeleteRecordFailed,
};

const char* DescribeChildFailure(EChildFailure failure)
{
    switch (failure) {
        case EChildFailure::EmptyLiveRecords:
            return "live records are empty";
        case EChildFailure::AllocRecordFailed:
            return "failed to allocate record";
        case EChildFailure::WriteRecordFailed:
            return "failed to write allocated record";
        case EChildFailure::CommitRecordFailed:
            return "failed to commit record";
        case EChildFailure::SelectedRecordInvalid:
            return "selected record is invalid";
        case EChildFailure::RewriteRecordFailed:
            return "failed to rewrite record";
        case EChildFailure::RewrittenRecordInvalid:
            return "rewritten record is invalid";
        case EChildFailure::DeleteRecordFailed:
            return "failed to delete record";
    }

    return "unknown child failure";
}

void ChildCheck(bool condition, EChildFailure failure)
{
    if (!condition) {
        _exit(static_cast<int>(failure));
    }
}

class TMutationWorker
{
private:
    TTable Table;
    TVector<ui64> LiveRecords;
    ui64 Salt = 0;
    ui64 Mutations = 0;
    bool Grow = true;

public:
    explicit TMutationWorker(const TString& tablePath)
        : Table(CreateTable(tablePath))
        , LiveRecords(ValidateAndCollectLiveRecords(Table))
        , Grow(LiveRecords.size() < TargetLiveRecords)
    {}

    void Run(EMutationStrategy strategy)
    {
        for (;;) {
            switch (strategy) {
                case EMutationStrategy::UpDown:
                    RunUpDownMutation();
                    break;
                case EMutationStrategy::Random:
                    RunRandomMutation();
                    break;
            }

            ++Mutations;
            if (Mutations % 64 == 0) {
                Table.TryDeallocateMemory();
            }
        }
    }

private:
    ui64 PickRecord()
    {
        ChildCheck(!LiveRecords.empty(), EChildFailure::EmptyLiveRecords);
        return LiveRecords[RandomNumber(LiveRecords.size())];
    }

    ui64 RemovePickedRecord()
    {
        ChildCheck(!LiveRecords.empty(), EChildFailure::EmptyLiveRecords);

        const ui64 pos = RandomNumber(LiveRecords.size());
        const ui64 index = LiveRecords[pos];
        LiveRecords[pos] = LiveRecords.back();
        LiveRecords.pop_back();
        return index;
    }

    ui32 RandomBodySize()
    {
        return 32 + RandomNumber<ui32>(2_KB);
    }

    void WriteNewRecord()
    {
        const TString record = MakeRecord(RandomBodySize(), ++Salt);

        auto result = Table.AllocRecord(record.size());
        ChildCheck(!HasError(result), EChildFailure::AllocRecordFailed);

        const ui64 index = result.GetResult();
        ChildCheck(
            Table.WriteRecordData(index, record.data(), record.size()),
            EChildFailure::WriteRecordFailed);
        ChildCheck(
            Table.CommitRecord(index),
            EChildFailure::CommitRecordFailed);
        LiveRecords.push_back(index);
    }

    void RewriteRecord()
    {
        const ui64 index = PickRecord();
        const TStringBuf oldRecord = Table.GetRecordWithValidation(index);
        ChildCheck(
            ValidateRecord(oldRecord),
            EChildFailure::SelectedRecordInvalid);

        const ui32 bodySize = oldRecord.size() - sizeof(TRecordHeader);
        const TString newRecord = MakeRecord(bodySize, ++Salt);
        ChildCheck(
            Table.WriteRecordData(index, newRecord.data(), newRecord.size()),
            EChildFailure::RewriteRecordFailed);
        ChildCheck(
            ValidateRecord(Table.GetRecordWithValidation(index)),
            EChildFailure::RewrittenRecordInvalid);
    }

    void DeleteRecord()
    {
        const ui64 index = RemovePickedRecord();
        ChildCheck(
            Table.DeleteRecord(index),
            EChildFailure::DeleteRecordFailed);
    }

    void RunUpDownMutation()
    {
        const ui64 action = RandomNumber<ui64>(100);

        if (LiveRecords.size() >= TargetLiveRecords) {
            Grow = false;
        } else if (LiveRecords.empty()) {
            Grow = true;
        }

        if (Grow && (LiveRecords.empty() || action < 90)) {
            WriteNewRecord();
        } else if (!Grow && !LiveRecords.empty() && action < 90) {
            DeleteRecord();
        } else if (!LiveRecords.empty()) {
            RewriteRecord();
        } else {
            WriteNewRecord();
        }
    }

    void RunRandomMutation()
    {
        const ui64 action = RandomNumber<ui64>(100);

        if (LiveRecords.empty() ||
            (LiveRecords.size() < TargetLiveRecords && action < 40))
        {
            WriteNewRecord();
        } else if (action < 60) {
            RewriteRecord();
        } else {
            DeleteRecord();
        }
    }
};

void RunWorker(const TString& tablePath, EMutationStrategy strategy)
{
    TMutationWorker worker(tablePath);
    worker.Run(strategy);
}

class TChildProcess
{
private:
    pid_t Pid = 0;

public:
    TChildProcess(const TString& tablePath, EMutationStrategy strategy)
    {
        Pid = ::fork();
        UNIT_ASSERT_C(Pid >= 0, LastSystemError());

        if (Pid == 0) {
            RunWorker(tablePath, strategy);
        }
    }

    TChildProcess(const TChildProcess&) = delete;
    TChildProcess& operator=(const TChildProcess&) = delete;

    ~TChildProcess()
    {
        if (Pid) {
            Kill();
        }
    }

    void Kill()
    {
        UNIT_ASSERT_VALUES_EQUAL_C(0, ::kill(Pid, SIGKILL), LastSystemError());

        int status = 0;
        UNIT_ASSERT_VALUES_EQUAL_C(
            Pid,
            ::waitpid(Pid, &status, 0),
            LastSystemError());
        if (WIFEXITED(status)) {
            const int exitCode = WEXITSTATUS(status);
            UNIT_FAIL(
                TStringBuilder()
                << "child exited before SIGKILL"
                << ": code=" << exitCode << " reason="
                << DescribeChildFailure(static_cast<EChildFailure>(exitCode)));
        }

        UNIT_ASSERT_C(
            WIFSIGNALED(status),
            TStringBuilder()
                << "child stopped before SIGKILL, status=" << status);
        UNIT_ASSERT_VALUES_EQUAL(SIGKILL, WTERMSIG(status));

        Pid = 0;
    }
};

void RunCrashCycle(const TString& tablePath, EMutationStrategy strategy)
{
    TChildProcess worker(tablePath, strategy);
    Sleep(CrashInterval);
    worker.Kill();

    ValidateTableFromFile(tablePath);
}

void RunCrashCycles(const TString& tablePath)
{
    const EMutationStrategy strategies[] = {
        EMutationStrategy::UpDown,
        EMutationStrategy::Random,
    };

    for (const auto strategy: strategies) {
        for (ui64 i = 0; i < CrashCyclesPerStrategy; ++i) {
            RunCrashCycle(tablePath, strategy);
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDynamicPersistentTableCrashTest)
{
    Y_UNIT_TEST(ShouldRecoverAfterProcessIsKilledDuringReadWriteDeleteLoop)
    {
        TTempDir tempDir;
        const TString tablePath = tempDir.Path() / "crash.table";

        RunCrashCycles(tablePath);
    }
}

}   // namespace NCloud
