#include "command.h"

#include <cloud/filestore/public/api/protos/locks.pb.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::ELockType ParseLockType(const TString& s)
{
    if (s == "shared") {
        return NProto::E_SHARED;
    }
    if (s == "exclusive") {
        return NProto::E_EXCLUSIVE;
    }
    ythrow yexception() << "invalid lock type: " << s
        << " (expected: shared|exclusive)";
}

NProto::ELockOrigin ParseLockOrigin(const TString& s)
{
    if (s == "fcntl") {
        return NProto::E_FCNTL;
    }
    if (s == "flock") {
        return NProto::E_FLOCK;
    }
    ythrow yexception() << "invalid lock origin: " << s
        << " (expected: fcntl|flock)";
}

////////////////////////////////////////////////////////////////////////////////
// Common part of acquirelock/releaselock/testlock: they address a file by
// path, open an IO handle on it and describe a byte range with an owner.

class TLockCommandBase: public TFileStoreCommand
{
protected:
    TString Path;
    ui64 Owner = 0;
    ui64 Offset = 0;
    ui64 Length = 0;
    i32 Pid = 0;
    TString LockOriginStr;

public:
    TLockCommandBase()
    {
        Opts.AddLongOption("path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&Path);

        Opts.AddLongOption("owner")
            .Required()
            .RequiredArgument("OWNER")
            .Help("lock owner id")
            .StoreResult(&Owner);

        Opts.AddLongOption("offset")
            .Optional()
            .RequiredArgument("OFFSET")
            .DefaultValue(0)
            .StoreResult(&Offset);

        Opts.AddLongOption("length")
            .Optional()
            .RequiredArgument("LENGTH")
            .Help("number of bytes to lock, 0 means till the end of file")
            .DefaultValue(0)
            .StoreResult(&Length);

        Opts.AddLongOption("pid")
            .Optional()
            .RequiredArgument("PID")
            .DefaultValue(0)
            .StoreResult(&Pid);

        Opts.AddLongOption("lock-origin")
            .Optional()
            .RequiredArgument("ORIGIN")
            .Help("fcntl|flock")
            .DefaultValue("fcntl")
            .StoreResult(&LockOriginStr);
    }

protected:
    struct TLockTarget
    {
        ui64 NodeId = 0;
        ui64 Handle = 0;
    };

    //
    // Handle access mode follows POSIX: a shared (read) lock needs read
    // access, an exclusive (write) lock needs write access. Callers pass
    // the flags matching the lock type they are about to use.
    //

    static int HandleFlags(NProto::ELockType lockType)
    {
        if (lockType == NProto::E_SHARED) {
            return ProtoFlag(NProto::TCreateHandleRequest::E_READ);
        }
        return ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);
    }

    TLockTarget OpenHandle(ISession& session, int flags)
    {
        const auto resolved = ResolvePath(session, Path, false);

        Y_ENSURE(
            resolved.back().Node.GetType() != NProto::E_DIRECTORY_NODE,
            "can't lock a directory node");

        Y_ABORT_UNLESS(resolved.size() >= 2);

        const auto& parent = resolved[resolved.size() - 2];

        auto request = CreateRequest<NProto::TCreateHandleRequest>();
        request->SetNodeId(parent.Node.GetId());
        request->SetName(ToString(resolved.back().Name));
        request->SetFlags(flags);

        auto response = WaitFor(
            session.CreateHandle(PrepareCallContext(), std::move(request)));

        CheckResponse(response);

        return {
            .NodeId = resolved.back().Node.GetId(),
            .Handle = response.GetHandle(),
        };
    }

    template <typename T>
    std::shared_ptr<T> CreateLockRequest(
        const TLockTarget& target,
        NProto::ELockOrigin origin)
    {
        auto request = CreateRequest<T>();
        request->SetNodeId(target.NodeId);
        request->SetHandle(target.Handle);
        request->SetOwner(Owner);
        request->SetOffset(Offset);
        request->SetLength(Length);
        request->SetPid(Pid);
        request->SetLockOrigin(origin);

        return request;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAcquireLockCommand final: public TLockCommandBase
{
private:
    TString LockTypeStr;

public:
    TAcquireLockCommand()
    {
        Opts.AddLongOption("lock-type")
            .Optional()
            .RequiredArgument("TYPE")
            .Help("shared|exclusive")
            .DefaultValue("exclusive")
            .StoreResult(&LockTypeStr);
    }

    bool Execute() override
    {
        // Validate the arguments before creating a session.
        const auto lockType = ParseLockType(LockTypeStr);
        const auto origin = ParseLockOrigin(LockOriginStr);

        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto target = OpenHandle(session, HandleFlags(lockType));

        auto request =
            CreateLockRequest<NProto::TAcquireLockRequest>(target, origin);
        request->SetLockType(lockType);

        auto response = WaitFor(
            session.AcquireLock(PrepareCallContext(), std::move(request)));

        CheckResponse(response);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReleaseLockCommand final: public TLockCommandBase
{
public:
    bool Execute() override
    {
        // Validate the arguments before creating a session.
        const auto origin = ParseLockOrigin(LockOriginStr);

        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        //
        // Unlocking has no access-mode requirement of its own; open for
        // read so that read-only files can be unlocked.
        //

        auto target = OpenHandle(
            session,
            HandleFlags(NProto::E_SHARED));

        auto request =
            CreateLockRequest<NProto::TReleaseLockRequest>(target, origin);

        auto response = WaitFor(
            session.ReleaseLock(PrepareCallContext(), std::move(request)));

        CheckResponse(response);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestLockCommand final: public TLockCommandBase
{
private:
    TString LockTypeStr;

public:
    TTestLockCommand()
    {
        Opts.AddLongOption("lock-type")
            .Optional()
            .RequiredArgument("TYPE")
            .Help("shared|exclusive")
            .DefaultValue("exclusive")
            .StoreResult(&LockTypeStr);
    }

    bool Execute() override
    {
        // Validate the arguments before creating a session.
        const auto lockType = ParseLockType(LockTypeStr);
        const auto origin = ParseLockOrigin(LockOriginStr);

        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto target = OpenHandle(session, HandleFlags(lockType));

        auto request =
            CreateLockRequest<NProto::TTestLockRequest>(target, origin);
        request->SetLockType(lockType);

        auto response = WaitFor(
            session.TestLock(PrepareCallContext(), std::move(request)));

        if (response.GetError().GetCode() == E_FS_WOULDBLOCK) {
            PrintConflict(response);
            return true;
        }

        CheckResponse(response);

        if (JsonOutput) {
            NJson::TJsonValue json;
            json.InsertValue("Compatible", true);
            Cout << NJson::WriteJson(json) << Endl;
        } else {
            Cout << "compatible" << Endl;
        }
        return true;
    }

private:
    void PrintConflict(const NProto::TTestLockResponse& response)
    {
        if (JsonOutput) {
            NJson::TJsonValue json;
            json.InsertValue("Compatible", false);
            json.InsertValue("Owner", response.GetOwner());
            json.InsertValue("Offset", response.GetOffset());
            json.InsertValue("Length", response.GetLength());
            if (response.HasLockType()) {
                json.InsertValue(
                    "LockType",
                    NProto::ELockType_Name(response.GetLockType()));
            }
            if (response.HasPid()) {
                json.InsertValue("Pid", response.GetPid());
            }
            if (response.HasIncompatibleLockOrigin()) {
                json.InsertValue(
                    "IncompatibleLockOrigin",
                    NProto::ELockOrigin_Name(
                        response.GetIncompatibleLockOrigin()));
            }
            Cout << NJson::WriteJson(json) << Endl;
        } else {
            Cout << "incompatible:"
                << " owner=" << response.GetOwner()
                << " offset=" << response.GetOffset()
                << " length=" << response.GetLength();
            if (response.HasLockType()) {
                Cout << " lock-type="
                    << NProto::ELockType_Name(response.GetLockType());
            }
            if (response.HasPid()) {
                Cout << " pid=" << response.GetPid();
            }
            if (response.HasIncompatibleLockOrigin()) {
                Cout << " lock-origin="
                    << NProto::ELockOrigin_Name(
                        response.GetIncompatibleLockOrigin());
            }
            Cout << Endl;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAcquireLockCommand()
{
    return std::make_shared<TAcquireLockCommand>();
}

TCommandPtr NewReleaseLockCommand()
{
    return std::make_shared<TReleaseLockCommand>();
}

TCommandPtr NewTestLockCommand()
{
    return std::make_shared<TTestLockCommand>();
}

}   // namespace NCloud::NFileStore::NClient
