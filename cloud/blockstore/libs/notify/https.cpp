#include "https.h"

#include <library/cpp/http/io/headers.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/list.h>
#include <util/generic/vector.h>
#include <util/network/pollerimpl.h>
#include <util/stream/str.h>
#include <util/string/printf.h>
#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>
#include <util/thread/factory.h>
#include <util/thread/lfstack.h>

#include <contrib/libs/curl/include/curl/curl.h>

/*
 *  In case there are some performance issues, try the following:
 *      1. use more than one multihandle (don't forget to enable curl_share)
 *      2. use more than one thread (maybe lock and unlock functions will be
 *          needed - can be passed to curl via some curl opts)
 */

namespace NCloud::NBlockStore::NNotify {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TMutexLocking
{
    using TMyMutex = TMutex;
};

class TSocketPoller: public TPollerImpl<TMutexLocking>
{
public:
    void AddWait(curl_socket_t sock, int what, void* cookie)
    {
        switch (what) {
            case CURL_POLL_IN:
                Set(cookie, sock, CONT_POLL_READ);
            break;
            case CURL_POLL_OUT:
                Set(cookie, sock, CONT_POLL_WRITE);
            break;
            case CURL_POLL_INOUT:
                Set(cookie, sock, CONT_POLL_READ | CONT_POLL_WRITE);
            break;
            case CURL_POLL_REMOVE:
                Remove(sock);
            break;
        }
    }

    size_t WaitD(
        void** events,
        int* filters,
        size_t len,
        const TInstant deadline)
    {
        return DoWait(events, filters, len, deadline);
    }

private:
    inline size_t DoWaitReal(
        void** ev,
        int* filters,
        TEvent* events,
        size_t len,
        const TInstant deadline)
    {
        size_t ret = TPollerImpl<TMutexLocking>::WaitD(events, len, deadline);

        for (size_t i = 0; i < ret; ++i) {
            ev[i] = ExtractEvent(&events[i]);
            filters[i] = ExtractFilter(&events[i]);
        }

        return ret;
    }

    inline size_t DoWait(
        void** ev,
        int* filters,
        size_t len,
        const TInstant deadline)
    {
        if (len == 1) {
            TEvent tmp;
            return DoWaitReal(ev, filters, &tmp, 1, deadline);
        } else {
            TTempArray<TEvent> events(len);
            return DoWaitReal(ev, filters, events.Data(), len, deadline);
        }
    }

    inline int ExtractFilter(TEvent* ev)
    {
        int ret = 0;
        int f = TPollerImpl<TMutexLocking>::ExtractFilter(ev);
        if (ExtractStatus(ev)) {
            return CURL_CSELECT_ERR;
        }
        if (f & CONT_POLL_READ) {
            ret |= CURL_CSELECT_IN;
        }
        if (f & CONT_POLL_WRITE) {
            ret |= CURL_CSELECT_OUT;
        }
        return ret;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEasyPool
{
public:
    TEasyPool()
    {
        Pool.reserve(1000);
    }

    ~TEasyPool()
    {
        with_lock (Lock) {
            for (auto h: Pool) {
                curl_easy_cleanup(h);
            }
        }
    }

    CURL* Create()
    {
        CURL* ret = nullptr;
        with_lock (Lock) {
            if (!Pool.empty()) {
                ret = Pool.back();
                Pool.pop_back();
            }
        }
        if (ret == nullptr) {
            ret = curl_easy_init();
        }
        return ret;
    }

    void Return(CURL* handle)
    {
        if (handle) {
            curl_easy_reset(handle);
            with_lock (Lock) {
                Pool.push_back(handle);
            }
        }
    }

private:
    TVector<CURL*> Pool;
    TAdaptiveLock Lock;
};

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxRetryCount = 3;
constexpr int MaxHandles = 64;
constexpr int RequestsByIteration = 32;

constexpr long MaxConnections = 6000L;
constexpr long MaxHostConnections = 60;

////////////////////////////////////////////////////////////////////////////////

struct TMultiHolder
{
    CURLM* Handle = nullptr;
    int StillRunning = 0;
    size_t Size = 0;
    TInstant Deadline = TInstant::Max();
    bool Started = false;
};

struct TEvent
{
    curl_socket_t Socket;

    TEvent(curl_socket_t s)
        : Socket(s)
    {
    }
};

enum class EHttpMethod
{
    Get,
    Post,
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void EasySetOpt(CURL* handle, CURLoption opt, T val)
{
    auto code = curl_easy_setopt(handle, opt, val);
    Y_ENSURE_EX(
        code == CURLE_OK,
        yexception() << "failed to set easy option " << static_cast<int>(opt));
}

template <typename T>
void MultiSetOpt(CURLM* handle, CURLMoption opt, T val)
{
    auto code = curl_multi_setopt(handle, opt, val);
    Y_DEBUG_ABORT_UNLESS(code == CURLM_OK);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class THttpsClient::TImpl
{
public:
    struct TCurlRequestHandle: TAtomicRefCount<TCurlRequestHandle>
    {
        struct TPostDataInfo
        {
            const char* Begin = nullptr;
            const char* Current = nullptr;
            const char* End = nullptr;
            std::atomic<bool> Cancelled = false;
        } PostDataInfo;

        struct TResponseInfo
        {
            TStringStream Data;
            THttpHeaders Headers;
        } WriteDataInfo;

        static size_t ReadFunction(
            char* buffer,
            size_t size,
            size_t nitems,
            void* userdata)
        {
            TPostDataInfo* data = static_cast<TPostDataInfo*>(userdata);
            if (data->Cancelled.load()) {
                return CURL_READFUNC_ABORT;
            }
            if (!size || !nitems) {
                return 0;
            }
            size_t len = Min(
                Min(size_t(data->End - data->Current), size * nitems),
                size_t(8192));
            memcpy(buffer, data->Current, len);
            data->Current += len;
            return len;
        }

        static size_t WriteFunction(
            char* ptr,
            size_t size,
            size_t nitems,
            TResponseInfo* responseInfo)
        {
            responseInfo->Data << TStringBuf(ptr, ptr + size * nitems);
            return size * nitems;
        }

        static size_t HeaderFunction(
            char* ptr,
            size_t size,
            size_t nitems,
            TResponseInfo* responseInfo)
        {
            TString line(ptr, size * nitems);
            if (!line.StartsWith("HTTP/") && line != "\r\n") {
                THttpInputHeader h(line);
                responseInfo->Headers.AddHeader(h);
            }
            return size * nitems;
        }

        static curl_socket_t OpenSocketFunction(
            void*,
            curlsocktype purpose,
            struct curl_sockaddr *address)
        {
            Y_DEBUG_ABORT_UNLESS(purpose == CURLSOCKTYPE_IPCXN);
            OpenConnections.fetch_add(1);
            return socket(address->family, address->socktype, address->protocol);
        }

        static int CloseSocketFunction(void*, curl_socket_t item)
        {
            OpenConnections.fetch_sub(1);
            return close(item);
        }

        static int SeekFunction(void *userp, curl_off_t offset, int origin)
        {
            TPostDataInfo* data = static_cast<TPostDataInfo*>(userp);
            Y_DEBUG_ABORT_UNLESS(origin == SEEK_SET);
            data->Current = data->Begin + offset;
            return CURL_SEEKFUNC_OK;
        }

        const ui64 Id;
        TImpl& Impl;
        TInstant StartTs = Now();
        CURL* Handle = nullptr;
        char ErrorString[CURL_ERROR_SIZE] = {0};
        TString Url;
        TString Data;
        EHttpMethod HttpMethod;
        curl_slist* Headers = nullptr;
        std::atomic<int> RetryCount = 0;
        THttpsCallback Callback;

        TCurlRequestHandle(
                ui64 id,
                TImpl& impl,
                const TString& url,
                const TString& data,
                EHttpMethod httpMethod,
                const THttpHeaders& headers,
                const THttpsCallback& callback)
            : Id(id)
            , Impl(impl)
            , Url(url)
            , Data(data)
            , HttpMethod(httpMethod)
            , Callback(callback)
        {

            Headers = curl_slist_append(Headers, "Expect:");

            switch (httpMethod) {
                case EHttpMethod::Get:
                    break;
                case EHttpMethod::Post: {
                    TString contentLength = Sprintf("Content-Length: %zu", Data.size());
                    Headers = curl_slist_append(Headers, contentLength.c_str());

                    break;
                }
            }

            for (const auto& h: headers) {
                const auto& str = h.ToString();
                Headers = curl_slist_append(Headers, str.c_str());
            }

            Init();

            RequestsInflight.fetch_add(1);
        }

        ~TCurlRequestHandle()
        {
            curl_slist_free_all(Headers);
            RequestsInflight.fetch_sub(1);
        }

        void Init()
        {
            Handle = Singleton<TEasyPool>()->Create();

            PostDataInfo.Begin = Data.begin();
            PostDataInfo.Current = Data.begin();
            PostDataInfo.End = Data.end();

            EasySetOpt(Handle, CURLOPT_HTTPHEADER, Headers);

            EasySetOpt(Handle, CURLOPT_ACCEPT_ENCODING, "");

            EasySetOpt(
                Handle,
                CURLOPT_WRITEFUNCTION,
                &TCurlRequestHandle::WriteFunction);
            EasySetOpt(Handle, CURLOPT_WRITEDATA, &WriteDataInfo);

            EasySetOpt(
                Handle,
                CURLOPT_HEADERFUNCTION,
                &TCurlRequestHandle::HeaderFunction);
            EasySetOpt(Handle, CURLOPT_HEADERDATA, &WriteDataInfo);

            EasySetOpt(
                Handle,
                CURLOPT_OPENSOCKETFUNCTION,
                &TCurlRequestHandle::OpenSocketFunction);
            EasySetOpt(
                Handle,
                CURLOPT_CLOSESOCKETFUNCTION,
                &TCurlRequestHandle::CloseSocketFunction);

            if (Impl.CaPath) {
                EasySetOpt(Handle, CURLOPT_CAPATH, Impl.CaPath.c_str());
            }

            EasySetOpt(Handle, CURLOPT_SSL_VERIFYPEER, 0);    // XXX
            EasySetOpt(Handle, CURLOPT_SSL_VERIFYHOST, Impl.VerifyHost);

            EasySetOpt(Handle, CURLOPT_VERBOSE, 0);

            EasySetOpt(Handle, CURLOPT_PRIVATE, this);
            EasySetOpt(Handle, CURLOPT_ERRORBUFFER, &ErrorString);

            EasySetOpt(Handle, CURLOPT_URL, Url.c_str());

            EasySetOpt(Handle, CURLOPT_KEEP_SENDING_ON_ERROR, 1);

            switch (HttpMethod) {
                case EHttpMethod::Get:
                    break;
                case EHttpMethod::Post:
                    EasySetOpt(Handle, CURLOPT_POST, 1);
                    EasySetOpt(
                        Handle,
                        CURLOPT_POSTFIELDSIZE,
                        static_cast<long>(Data.size()));
                    EasySetOpt(Handle, CURLOPT_POSTFIELDS, Data.data());
                    break;
            }
        }

        void Retry()
        {
            if (Handle) {
                Singleton<TEasyPool>()->Return(Handle);
            }
            Init();
            RetryCount++;
        }

        void Cancel()
        {
            bool expected = false;
            if (PostDataInfo.Cancelled.compare_exchange_strong(expected, true)) {
                return;
            }
            SetError("cancelled");
        }

        bool IsCancelled() const
        {
            return PostDataInfo.Cancelled.load();
        }

        void SetError(const TString& e)
        {
            Callback(0, e);
        }
    };

    using TCurlRequestHandlePtr = TIntrusivePtr<TCurlRequestHandle>;

    struct TUserData
    {
        TImpl* Impl = nullptr;
    };

    TCurlRequestHandlePtr SendRequest(
        EHttpMethod httpMethod,
        const TString& endpoint,
        const TString& data,
        const THttpHeaders& headers,
        const THttpsCallback& callback,
        ui64 reqId);

    void Enqueue(TCurlRequestHandle* req)
    {
        AddQueue.Enqueue(req);
        InFlight++;
        Signal();
    }

    static int SocketFunction(
        CURL* easyHandle,
        curl_socket_t sock,
        int what,
        TUserData* userp,
        TEvent* socketp);

    static int TimerFunction(
        CURLM* multi,
        long timeoutMs,
        TInstant* userp);

    void InitMultiHolder();

    void Perform();
    void ResetSignalled();
    void ProcessAddQueue();
    void ProcessRemoveQueue();
    void Process();

    void Signal();

    TImpl();
    ~TImpl();

    void SetCaCerts(const TString& caPath);
    void SetVerifyHost(bool verifyHost);
    void SendRequest(
        EHttpMethod httpMethod,
        const TString& endpoint,
        const TString& data,
        const THttpHeaders& headers,
        const THttpsCallback& callback);

private:
    std::atomic<int> Running;

    TString CaPath;
    bool VerifyHost = true;

    std::atomic<ui64> RequestId;

    TMultiHolder MultiHolder;
    TList<TUserData> UserData;
    THolder<TSocketPoller> SocketPoller;
    TVector<TCurlRequestHandle*> CurlRemoveQueue;
    TSocketHolder SignalSockets[2];
    std::atomic<bool> Signalled;
    THolder<IThreadFactory::IThread> Thread;
    TManualEvent StartEvent;

    std::atomic<size_t> InFlight;

    TLockFreeStack<TCurlRequestHandle*> AddQueue;
    TVector<TCurlRequestHandle*> AddVector;
    THashSet<TCurlRequestHandle*> InFlightHandles;

public:
    static std::atomic<int> RequestsInflight;
    static std::atomic<int> OpenConnections;
};

void THttpsClient::TImpl::InitMultiHolder()
{
    MultiHolder.Handle = curl_multi_init();
    UserData.push_back(TUserData{this});
    auto ud = &UserData.back();

    MultiSetOpt(MultiHolder.Handle, CURLMOPT_MAXCONNECTS, MaxConnections);
    MultiSetOpt(
        MultiHolder.Handle,
        CURLMOPT_MAX_HOST_CONNECTIONS,
        MaxHostConnections);

    MultiSetOpt(
        MultiHolder.Handle,
        CURLMOPT_SOCKETFUNCTION,
        &TImpl::SocketFunction);
    MultiSetOpt(MultiHolder.Handle, CURLMOPT_SOCKETDATA, ud);

    MultiSetOpt(
        MultiHolder.Handle,
        CURLMOPT_TIMERFUNCTION,
        &TImpl::TimerFunction);
    MultiSetOpt(MultiHolder.Handle, CURLMOPT_TIMERDATA, &MultiHolder.Deadline);
}

void THttpsClient::TImpl::ProcessAddQueue() {
    if (AddVector.empty()) {
        AddQueue.DequeueAllSingleConsumer(&AddVector);
    }

    while (!AddVector.empty()) {
        if (MultiHolder.Size >= MaxHandles) {
            SocketPoller->AddWait(SignalSockets[0], CURL_POLL_REMOVE, nullptr);
            return;
        }

        while (MultiHolder.Size < MaxHandles && AddVector.size() > 0) {
            auto* req = AddVector.back();
            AddVector.pop_back();

            auto* easyHandle = req->Handle;
            if (req->IsCancelled()) {
                Singleton<TEasyPool>()->Return(easyHandle);
                req->UnRef();
                InFlight--;
            } else {
                curl_multi_add_handle(MultiHolder.Handle, easyHandle);
                InFlightHandles.insert(req);
                MultiHolder.Size++;
            }
        }

        if (AddVector.size() == 0) {
            SocketPoller->AddWait(SignalSockets[0], CURL_POLL_IN, nullptr);
        }

        if ((MultiHolder.Size > 0 && !MultiHolder.Started)
                || MultiHolder.Deadline == TInstant::Zero())
        {
            auto status = curl_multi_socket_action(
                MultiHolder.Handle,
                CURL_SOCKET_TIMEOUT,
                0,
                &MultiHolder.StillRunning);
            Y_DEBUG_ABORT_UNLESS(status == CURLM_OK);
            MultiHolder.Started = true;
            Process();
        }
    }
}

void THttpsClient::TImpl::ProcessRemoveQueue()
{
    auto releaseReq = [this] (TCurlRequestHandle* req) {
        auto* e = req->Handle;

        auto code = curl_multi_remove_handle(MultiHolder.Handle, e);
        Y_DEBUG_ABORT_UNLESS(code == CURLM_OK);
        Singleton<TEasyPool>()->Return(e);
        if (--MultiHolder.Size == 0) {
            MultiHolder.Started = false;
            MultiHolder.Deadline = TInstant::Max();
        }
        Y_DEBUG_ABORT_UNLESS(InFlight > 0);
        InFlight--;

        req->UnRef();
    };

    if (CurlRemoveQueue.size() > 0) {
        for (auto req: CurlRemoveQueue) {
            InFlightHandles.erase(req);
            releaseReq(req);
        }
        CurlRemoveQueue.clear();
    }

    // curl_multi_remove_handle call may be expensive,
    // so make sure we wouldn't block event loop for too long
    const auto deadline = TDuration::MilliSeconds(5).ToDeadLine();

    if (InFlightHandles.size() > 0) {
        auto it = InFlightHandles.begin();
        while ( it != InFlightHandles.end() && Now() < deadline) {
            auto req = *it;
            if (Y_UNLIKELY(req->IsCancelled())) {
                InFlightHandles.erase(it++);
                releaseReq(req);
            } else {
                ++it;
            }
        }
    }
}

void THttpsClient::TImpl::ResetSignalled()
{
    if (AddVector.empty() && Signalled.load()) {
        char buf[1];
        auto bytes = read(SignalSockets[0], buf, sizeof(buf));
        Y_DEBUG_ABORT_UNLESS(bytes == sizeof(buf));
        Signalled.store(false);
    }
}

void THttpsClient::TImpl::Perform()
{
    void* events[RequestsByIteration];
    int filters[RequestsByIteration];

    auto minDeadline = MultiHolder.Deadline;

    if (auto d = Now() + TDuration::Seconds(1); minDeadline > d) {
        minDeadline = d;
    }

    auto count =
        SocketPoller->WaitD(events, filters, RequestsByIteration, minDeadline);

    if (count > 0) {
        // we should read all events beforehand because
        // curl_multi_socket_action can destroy some of them with socket function call
        TVector<std::pair<TEvent, int>> sockets;
        sockets.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            if (events[i] != nullptr) {
                sockets.emplace_back(
                    *static_cast<TEvent*>(events[i]), filters[i]);
            }
        }
        for (auto& e: sockets) {
            auto fd = e.first.Socket;
            auto code = curl_multi_socket_action(
                MultiHolder.Handle,
                fd,
                e.second,
                &MultiHolder.StillRunning);
            Y_DEBUG_ABORT_UNLESS(code == CURLM_OK);
            Process();
        }
    } else {
        auto now = Now();
        if (MultiHolder.Deadline < now) {
            MultiHolder.Deadline = TInstant::Max();
            if (MultiHolder.Size) {
                auto code = curl_multi_socket_action(
                    MultiHolder.Handle,
                    CURL_SOCKET_TIMEOUT,
                    0,
                    &MultiHolder.StillRunning);
                Y_DEBUG_ABORT_UNLESS(code == CURLM_OK);
                Process();
            }
        }
    }
}

void THttpsClient::TImpl::Process()
{
    int msgsInQueue;
    while (auto curlMsg = curl_multi_info_read(MultiHolder.Handle, &msgsInQueue)) {
        Y_DEBUG_ABORT_UNLESS(curlMsg->msg == CURLMSG_DONE);

        TCurlRequestHandle* req = nullptr;

        auto ret =
            curl_easy_getinfo(curlMsg->easy_handle, CURLINFO_PRIVATE, &req);
        Y_DEBUG_ABORT_UNLESS(ret == CURLE_OK);
        Y_DEBUG_ABORT_UNLESS(req != nullptr);

        if (curlMsg->msg == CURLMSG_DONE && ret == CURLE_OK) {
            auto result = curlMsg->data.result;
            if (result == CURLE_OK) {
                curl_off_t totaltime = 0;
                auto code = curl_easy_getinfo(
                    curlMsg->easy_handle,
                    CURLINFO_TOTAL_TIME_T,
                    &totaltime);
                Y_DEBUG_ABORT_UNLESS(code == CURLE_OK);
                long retCode = 0;
                code = curl_easy_getinfo(
                    curlMsg->easy_handle,
                    CURLINFO_RESPONSE_CODE,
                    &retCode);
                Y_DEBUG_ABORT_UNLESS(code == CURLE_OK);
                req->Callback(retCode, req->WriteDataInfo.Data.Str());
            } else if ((result == CURLE_SEND_ERROR || result == CURLE_RECV_ERROR)
                    && req->RetryCount < MaxRetryCount)
            {
                auto code = curl_multi_remove_handle(
                    MultiHolder.Handle,
                    curlMsg->easy_handle);
                Y_DEBUG_ABORT_UNLESS(code == CURLM_OK);
                req->Retry();
                code = curl_multi_add_handle(MultiHolder.Handle, req->Handle);
                Y_DEBUG_ABORT_UNLESS(code == CURLM_OK);
                continue;
            } else {
                req->SetError(req->ErrorString);
            }
        }
        CurlRemoveQueue.push_back(req);
    }
}

struct TCurlInit
{
    TCurlInit()
    {
        auto res = curl_global_init(CURL_GLOBAL_DEFAULT);
        Y_ENSURE(res == 0);
    }

    ~TCurlInit() {
        curl_global_cleanup();
    }
};

int THttpsClient::TImpl::SocketFunction(
    CURL* easyHandle,
    curl_socket_t sock,
    int what,
    TUserData* userp,
    TEvent* e)
{
    Y_UNUSED(easyHandle);

    auto* impl = userp->Impl;

    if (what == CURL_POLL_REMOVE) {
        delete e;
        e = nullptr;
    } else if (!e) {
        e = new TEvent(sock);
        auto code = curl_multi_assign(impl->MultiHolder.Handle, sock, e);
        Y_DEBUG_ABORT_UNLESS(code == CURLM_OK);
    }

    impl->SocketPoller->AddWait(sock, what, e);

    return CURLM_OK;
}

int THttpsClient::TImpl::TimerFunction(CURLM*, long timeoutMs, TInstant* userp)
{
    Y_DEBUG_ABORT_UNLESS(userp != nullptr);

    switch (timeoutMs) {
        case -1:
            *userp = TInstant::Max();
            break;
        case 0:
            *userp = TInstant::Zero();
            break;
        default:
            *userp = TDuration::MilliSeconds(timeoutMs).ToDeadLine();
            break;
    }

    return CURLM_OK;
}

THttpsClient::TImpl::TImpl()
    : RequestId(0)
    , SocketPoller(new TSocketPoller())
    , InFlight(0)
{
    static auto curlInit = Singleton<TCurlInit>();
    Y_UNUSED(curlInit);

    Running.store(true);

    SOCKET fds[2];
    Y_ENSURE(SocketPair(fds) == 0, yexception() << strerror(errno));
    TSocketHolder readSocket = fds[0];
    TSocketHolder writeSocket = fds[1];
    SignalSockets[0].Swap(readSocket);
    SignalSockets[1].Swap(writeSocket);
    SocketPoller->AddWait(SignalSockets[0], CURL_POLL_IN, nullptr);
    Signalled.store(false);

    InitMultiHolder();

    InFlightHandles.reserve(MaxHandles);

    Thread.Reset(SystemThreadFactory()->Run([this]() {
        TThread::SetCurrentThreadName("Curl");
        while (Running.load() || InFlight) {
            ProcessRemoveQueue();
            ProcessAddQueue();
            ResetSignalled();
            Perform();
        }
    }).Release());
}

THttpsClient::TImpl::~TImpl()
{
    Running.store(false);

    Signal();
    if (Thread) {
        Thread->Join();
    }

    CURLMcode ret = curl_multi_cleanup(MultiHolder.Handle);
    Y_DEBUG_ABORT_UNLESS(ret == CURLM_OK);
}

void THttpsClient::TImpl::SetCaCerts(const TString& caPath)
{
    CaPath = caPath;
}

void THttpsClient::TImpl::SetVerifyHost(bool verifyHost)
{
    VerifyHost = verifyHost;
}

void THttpsClient::TImpl::SendRequest(
    EHttpMethod httpMethod,
    const TString& endpoint,
    const TString& data,
    const THttpHeaders& headers,
    const THttpsCallback& callback)
{
    try {
        TCurlRequestHandlePtr ret = new TCurlRequestHandle(
            RequestId.fetch_add(1),
            *this,
            endpoint,
            data,
            httpMethod,
            headers,
            callback);
        ret->Ref();
        Enqueue(&*ret);
    } catch (const yexception& e) {
        callback(0, e.what());
    }
}

void THttpsClient::TImpl::Signal()
{
    bool expected = false;
    if (Signalled.compare_exchange_strong(expected, true)) {
        static const char buf[1] = {0};
        auto bytes = write(SignalSockets[1], buf, 1);
        Y_DEBUG_ABORT_UNLESS(bytes == sizeof(buf));
    }
}

std::atomic<int> THttpsClient::TImpl::RequestsInflight = 0;
std::atomic<int> THttpsClient::TImpl::OpenConnections = 0;

////////////////////////////////////////////////////////////////////////////////

THttpsClient::THttpsClient()
    : Impl(std::make_unique<TImpl>())
{
}

THttpsClient::~THttpsClient()
{
}

void THttpsClient::LoadCaCerts(const TString& path)
{
    Impl->SetCaCerts(path);
}

void THttpsClient::Post(
    const TString& endpoint,
    const TString& data,
    const TString& contentType,
    const TString& authHeader,
    const THttpsCallback& callback)
{
    THttpHeaders headers;
    headers.AddHeader(THttpInputHeader("Content-Type", contentType));
    if (!authHeader.empty()) {
        headers.AddHeader(THttpInputHeader("Authorization", "Bearer " + authHeader));
    }
    Impl->SendRequest(
        EHttpMethod::Post,
        endpoint,
        data,
        headers,
        callback);
}

}   // namespace NCloud::NBlockStore::NNotify
