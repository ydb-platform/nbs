#include "socket_worker.h"

#include <library/cpp/testing/unittest/registar.h>

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>

namespace NCloud::NIcV2 {

namespace {

class TSelfStoppingHandler final: public ISocketWorkerHandler
{
public:
    TSocketWorker Worker{*this};

    void Run() override
    {
        Worker.Stop();
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2SocketWorkerTest)
{
    SIMPLE_UNIT_FORKED_TEST(RejectsStopFromWorkerBeforeWaitingForExternalStop)
    {
        const pid_t child = fork();
        UNIT_ASSERT(child >= 0);
        if (child == 0) {
            alarm(5);
            TSelfStoppingHandler handler;
            handler.Worker.Start();
            handler.Worker.Stop();
            _exit(0);
        }
        int status = 0;
        pid_t result;
        do {
            result = waitpid(child, &status, 0);
        } while (result < 0 && errno == EINTR);
        UNIT_ASSERT_VALUES_EQUAL(result, child);
        UNIT_ASSERT(WIFSIGNALED(status));
        UNIT_ASSERT_VALUES_EQUAL(WTERMSIG(status), SIGABRT);
    }
}

}   // namespace NCloud::NIcV2
