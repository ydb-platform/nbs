#include <cloud/storage/core/tools/testing/silk_demo/protos/silk_demo.pb.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/system/file.h>

#include <cerrno>
#include <system_error>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace NCloud::NStorage::NSilkDemo;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto ErrnoMsg(int err)
{
    return std::system_category().message(err);
}

////////////////////////////////////////////////////////////////////////////////
// Blocking TCP helpers (client doesn't need silk).

int RecvAll(int fd, void* buf, size_t len)
{
    auto* p = static_cast<ui8*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::recv(fd, p + done, len - done, 0);
        if (n > 0) {
            done += static_cast<size_t>(n);
        } else if (n == 0) {
            return EIO;
        } else if (errno != EINTR) {
            return errno;
        }
    }
    return 0;
}

int SendAll(int fd, const void* buf, size_t len)
{
    const auto* p = static_cast<const ui8*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::send(fd, p + done, len - done, MSG_NOSIGNAL);
        if (n > 0) {
            done += static_cast<size_t>(n);
        } else if (errno != EINTR) {
            return errno;
        }
    }
    return 0;
}

int ConnectTo(const TString& host, ui16 port, int* outFd)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return errno;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        return EINVAL;
    }

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        int e = errno;
        ::close(fd);
        return e;
    }
    *outFd = fd;
    return 0;
}

int SendRequest(int fd, const TRequest& req, TResponse* resp)
{
    TString reqBuf;
    if (!req.SerializeToString(&reqBuf)) {
        return EBADMSG;
    }

    ui32 lenBe = htonl(static_cast<ui32>(reqBuf.size()));
    if (int r = SendAll(fd, &lenBe, sizeof(lenBe)); r) {
        return r;
    }
    if (int r = SendAll(fd, reqBuf.data(), reqBuf.size()); r) {
        return r;
    }

    ui32 respLenBe = 0;
    if (int r = RecvAll(fd, &respLenBe, sizeof(respLenBe)); r) {
        return r;
    }
    ui32 respLen = ntohl(respLenBe);
    if (respLen > 64_MB) {
        return EMSGSIZE;
    }

    TString respBuf;
    respBuf.ReserveAndResize(respLen);
    if (int r = RecvAll(fd, respBuf.begin(), respLen); r) {
        return r;
    }
    if (!resp->ParseFromString(respBuf)) {
        return EBADMSG;
    }
    return 0;
}

int Fail(const TString& msg)
{
    Cerr << "error: " << msg << Endl;
    return 1;
}

TString ReadInput(const TString& path)
{
    if (path == "-") {
        return Cin.ReadAll();
    }
    return TUnbufferedFileInput(path).ReadAll();
}

}   // namespace

int main(int argc, const char** argv)
{
    using namespace NLastGetopt;

    TString host;
    ui16 port = 0;
    TString mode;
    TString path;
    ui64 offset = 0;
    ui64 length = 0;
    TString dataFile;

    {
        TOpts opts;
        opts.AddHelpOption();
        opts.AddLongOption("host", "server host")
            .DefaultValue("127.0.0.1")
            .StoreResult(&host);
        opts.AddLongOption("port", "server port")
            .Required()
            .StoreResult(&port);
        opts.AddLongOption("mode", "read|write")
            .Required()
            .StoreResult(&mode);
        opts.AddLongOption("path", "remote file path")
            .Required()
            .StoreResult(&path);
        opts.AddLongOption("offset", "byte offset")
            .DefaultValue(0)
            .StoreResult(&offset);
        opts.AddLongOption("length", "bytes to read (read mode)")
            .DefaultValue(0)
            .StoreResult(&length);
        opts.AddLongOption(
                "data-file",
                "local file to read payload from for write mode;"
                " '-' reads stdin")
            .DefaultValue("-")
            .StoreResult(&dataFile);
        TOptsParseResultException(&opts, argc, argv);
    }

    int fd = -1;
    if (int r = ConnectTo(host, port, &fd)) {
        return Fail(TStringBuilder() << "connect: " << ErrnoMsg(r));
    }

    TRequest req;
    if (mode == "write") {
        TString payload = ReadInput(dataFile);
        auto* w = req.MutableWrite();
        w->SetPath(path);
        w->SetOffset(offset);
        w->SetData(std::move(payload));
    } else if (mode == "read") {
        auto* r = req.MutableRead();
        r->SetPath(path);
        r->SetOffset(offset);
        r->SetLength(length);
    } else {
        ::close(fd);
        return Fail("--mode must be read or write");
    }

    TResponse resp;
    int r = SendRequest(fd, req, &resp);
    ::close(fd);
    if (r) {
        return Fail(TStringBuilder() << "rpc: " << ErrnoMsg(r));
    }

    if (resp.HasError() && resp.GetError().GetCode() != 0) {
        return Fail(TStringBuilder()
            << "server error " << resp.GetError().GetCode()
            << ": " << resp.GetError().GetMessage());
    }

    switch (resp.GetBodyCase()) {
        case TResponse::kWrite:
            Cerr << "wrote " << resp.GetWrite().GetBytesWritten()
                << " bytes" << Endl;
            break;
        case TResponse::kRead:
            Cout.Write(
                resp.GetRead().GetData().data(),
                resp.GetRead().GetData().size());
            Cerr << "read " << resp.GetRead().GetData().size()
                << " bytes" << Endl;
            break;
        default:
            return Fail("empty response body");
    }

    return 0;
}
