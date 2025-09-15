#include "env.h"

#include "env_impl.h"
#include "spdk.h"

#include <cloud/filestore/libs/spdk/iface/config.h>

#include <library/cpp/logger/log.h>

#include <util/stream/printf.h>
#include <util/system/src_location.h>
#include <util/system/thread.h>

namespace NCloud::NFileStore::NSpdk {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TLog Log { CreateLogBackend("console", TLOG_WARNING) };

ELogPriority LogLevelToPriority(int level)
{
    switch (level) {
        case SPDK_LOG_ERROR:    return TLOG_ERR;
        case SPDK_LOG_WARN:     return TLOG_WARNING;
        case SPDK_LOG_NOTICE:   return TLOG_NOTICE;
        case SPDK_LOG_INFO:     return TLOG_INFO;

        default:
        case SPDK_LOG_DEBUG:    return TLOG_DEBUG;
    }
}

Y_DECLARE_UNUSED void LogMessage(
    int level,
    const char* file,
    const int line,
    const char* func,
    const char* format,
    va_list args)
{
    auto priority = LogLevelToPriority(level);
    if (priority <= Log.FiltrationLevel()) {
        auto fileName = ::NPrivate::StripRoot({ file, (ui32)strlen(file) });
        Printf(
            Log << priority << TSourceLocation(fileName, line)
                << ": (" << func << ") ",
            format, args);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCoreTaskQueue final
    : public ITaskQueue
{
private:
    const ui32 Core;

public:
    explicit TCoreTaskQueue(ui32 core)
        : Core(core)
    {}

    void Start() override
    {
    }

    void Stop() override
    {
    }

    void Enqueue(ITaskPtr task) override
    {
        auto* event = spdk_event_allocate(
            Core,
            &ExecuteTask,
            task.get(),
            nullptr);

        if (!event) {
            ythrow yexception() << "unable to allocate SPDK event";
        }

        spdk_event_call(event);
        task.release();    // will be deleted in callback
    }

private:
    static void ExecuteTask(void* arg1, void* arg2)
    {
        Y_UNUSED(arg2);

        ITaskPtr task(reinterpret_cast<ITask*>(arg1));
        task->Execute();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TSpdkEnv::TMainThread final
    : public ISimpleThread
{
private:
    const TSpdkEnvConfigPtr Config;

    TPromise<void> StartResult = NewPromise();
    TPromise<void> StopResult = NewPromise();

    TVector<TCoreTaskQueue> TaskQueues;

    TAtomic MainCoreIdx = 0;
    TAtomic PrevCoreIdx = -1;

public:
    explicit TMainThread(TSpdkEnvConfigPtr config)
        : Config(std::move(config))
    {}

    TFuture<void> Start()
    {
        if (!StartResult.HasValue()) {
            ISimpleThread::Start();
        }
        return StartResult;
    }

    TFuture<void> Stop()
    {
        if (!StopResult.HasValue()) {
            spdk_app_start_shutdown();
        }
        return StopResult;
    }

    ITaskQueue* GetTaskQueue(int index = -1)
    {
        if (index < 0) {
            index = AtomicGet(MainCoreIdx);
        }
        return &TaskQueues[index];
    }

    int PickCore()
    {
        for (;;) {
            auto old = AtomicGet(PrevCoreIdx);

            auto idx = old + 1;
            if (idx >= TaskQueues.ysize()) {
                idx = 0;
            }

            if (AtomicCas(&PrevCoreIdx, idx, old)) {
                return idx;
            }
        }
    }

private:
    void* ThreadProc() override
    {
        spdk_app_opts opts;
        spdk_app_opts_init(&opts, sizeof(opts));

        // can be used to pass extra DPDK options
        char env_context[] = "--no-telemetry";

        opts.log = LogMessage;
        opts.enable_coredump = false;
        opts.rpc_addr = nullptr;    // disable RPC listener
        opts.env_context = env_context;

        auto cpuMask = Config->GetCpuMask();
        if (cpuMask) {
            opts.reactor_mask = cpuMask.c_str();
        }

        auto hugeDir = Config->GetHugeDir();
        if (hugeDir) {
            opts.hugedir = hugeDir.c_str();
        }

        int rc = spdk_app_start(&opts, ExecuteOnStart, this);
        if (rc) {
            StartResult.SetException("unable to initialize SPDK library");
        }

        spdk_app_fini();

        StopResult.SetValue();
        return nullptr;
    }

    void OnStart()
    {
        AtomicSet(MainCoreIdx, spdk_env_get_core_index(-1));

        ui32 core;
        SPDK_ENV_FOREACH_CORE(core) {
            TaskQueues.emplace_back(core);
        }

        StartResult.SetValue();
    }

    static void ExecuteOnStart(void* arg1)
    {
        reinterpret_cast<TMainThread*>(arg1)->OnStart();
    }
};

////////////////////////////////////////////////////////////////////////////////

TSpdkEnv::TSpdkEnv(TSpdkEnvConfigPtr config)
    : MainThread(new TMainThread(std::move(config)))
{}

TSpdkEnv::~TSpdkEnv()
{}

void TSpdkEnv::Start()
{
    StartAsync().GetValue(StartTimeout);
}

void TSpdkEnv::Stop()
{
    StopAsync().GetValue(StartTimeout);
}

TFuture<void> TSpdkEnv::StartAsync()
{
    return MainThread->Start();
}

TFuture<void> TSpdkEnv::StopAsync()
{
    return MainThread->Stop();
}

void TSpdkEnv::Enqueue(ITaskPtr task)
{
    MainThread->GetTaskQueue()->Enqueue(std::move(task));
}

int TSpdkEnv::PickCore()
{
    return MainThread->PickCore();
}

ITaskQueue* TSpdkEnv::GetTaskQueue(int index)
{
    return MainThread->GetTaskQueue(index);
}

////////////////////////////////////////////////////////////////////////////////

void InitLogging(const TLog& log)
{
    Log = log;

    static const char* components[] = {
        "bdev",
        "bdev_nvme",
        "memory",
        "nvme",
        "nvmf",
        "nvmf_tcp",
        "rdma",
    };

    for (auto* c: components) {
       spdk_log_set_flag(c);
    }
}

ISpdkEnvPtr CreateEnv(TSpdkEnvConfigPtr config)
{
    return std::make_shared<TSpdkEnv>(std::move(config));
}

}   // namespace NCloud::NFileStore::NSpdk
