#include "bootstrap.h"

#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/libs/vfs_fuse/loop.h>

#include <cloud/storage/core/libs/daemon/app.h>

#include <contrib/ydb/core/driver_lib/run/factories.h>
#include <contrib/ydb/core/mind/lease_holder.h>
#include <contrib/ydb/public/api/protos/ydb_discovery.pb.h>
#include <contrib/ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpcpp/security/server_credentials.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/tempdir.h>
#include <util/generic/cast.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/string/printf.h>

#include <memory>
#include <string>

namespace NCloud::NFileStore::NDaemon {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString SysConfigStr = R"(
    Executor {
        Type: BASIC
        Threads: 1
        Name: "System"
    }
    Executor {
        Type: BASIC
        Threads: 1
        Name: "User"
    }
    Executor {
        Type: BASIC
        Threads: 1
        Name: "Batch"
    }
    Executor {
        Type: IO
        Threads: 1
        Name: "IO"
    }
    Executor {
        Type: BASIC
        Threads: 1
        Name: "IC"
    }
    Scheduler {
        Resolution: 64
        SpinThreshold: 0
        ProgressThreshold: 10000
    }
    SysExecutor: 0
    UserExecutor: 1
    IoExecutor: 3
    BatchExecutor: 2
    ServiceExecutor {
        ServiceName: "User"
        ExecutorId: 1
    }
    ServiceExecutor {
        ServiceName: "Batch"
        ExecutorId: 2
    }
    ServiceExecutor {
        ServiceName: "IO"
        ExecutorId: 3
    }
    ServiceExecutor {
        ServiceName: "Interconnect"
        ExecutorId: 4
    }
)";

const TString StorageConfigTemplateStr = R"(
    SchemeShardDir: "local"
    NodeType: "nfs"
    NodeRegistrationRootCertsFile: "%s"
    NodeRegistrationCert {
        CertFile: "%s"
        CertPrivateKeyFile: "%s"
    }
)";

const TString NamingConfigTemplateStr = R"(
    Node {
        NodeId: 1
        Address: "localhost"
        Host: "localhost"
        Port: %d
        InterconnectHost: "localhost"
    }
)";

const TString DomainsConfigStr = R"(
    Domain {
        DomainId: 1
        SchemeRoot: 72057594046678944
        SSId: 1
        HiveUid: 1
        Name: "local"
        StoragePoolTypes {
            Kind: "ssd"
            PoolConfig {
                BoxId: 1
                ErasureSpecies: "block-4-2"
                VDiskKind: "Default"
                Kind: "ssd"
                PDiskFilter {
                    Property {
                        Type: SSD
                    }
                }
            }
        }
        ExplicitMediators: 72057594046382081
        ExplicitCoordinators: 72057594046316545
        ExplicitAllocators: 72057594046447617
    }
    StateStorage {
        SSId: 1
        Ring {
            NToSelect: 1
            Node: 1
        }
    }
    HiveConfig {
        HiveUid: 1
        Hive: 72057594037968897
    }
)";

////////////////////////////////////////////////////////////////////////////////

TString GetTestFilePath(const TString& fileName)
{
    return JoinFsPaths(ArcadiaSourceRoot(), "cloud/filestore/tests", fileName);
}

TString GetCertFile()
{
    return GetTestFilePath("certs/server.crt");
}

TString GetCertKeyFile()
{
    return GetTestFilePath("certs/server.key");
}

TString ReadFile(const TString& fileName)
{
    TFileInput in(fileName);
    return in.ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

auto MakeSslServerCredentials()
{
    grpc::SslServerCredentialsOptions opts;
    opts.client_certificate_request =
        GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;

    const auto certFile = GetCertFile();
    const auto certKeyFile = GetCertKeyFile();

    opts.pem_root_certs = ReadFile(certFile);

    grpc::SslServerCredentialsOptions::PemKeyCertPair keyCert;
    keyCert.cert_chain = ReadFile(certFile);
    keyCert.private_key = ReadFile(certKeyFile);
    opts.pem_key_cert_pairs.push_back(keyCert);

    return grpc::SslServerCredentials(opts);
}

////////////////////////////////////////////////////////////////////////////////

// Needed to mock NodeBroker to allow our dynamic node to register
class TFakeYdbDiscoveryService final
    : public Ydb::Discovery::V1::DiscoveryService::Service
{
private:
    ui64 DynamicNodeIcPort;

public:
    explicit TFakeYdbDiscoveryService(ui64 dynamicNodeIcPort)
        : DynamicNodeIcPort(dynamicNodeIcPort)
    {}

    grpc::Status NodeRegistration(
        grpc::ServerContext* ctx,
        const Ydb::Discovery::NodeRegistrationRequest* req,
        Ydb::Discovery::NodeRegistrationResponse* resp) override
    {
        Y_UNUSED(ctx);
        Y_UNUSED(req);

        auto* operation = resp->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);

        Ydb::Discovery::NodeRegistrationResult result;
        result.set_node_id(1);
        result.set_scope_tablet_id(72057594037968897);
        result.set_scope_path_id(1);

        auto* node = result.add_nodes();
        node->set_node_id(2);
        node->set_address("localhost");
        node->set_host("localhost");
        node->set_port(DynamicNodeIcPort);

        operation->mutable_result()->PackFrom(result);

        return grpc::Status::OK;
    }
};

// Grpc server for TFakeYdbDiscoveryService.
// Needed to mock NodeBroker to allow our dynamic node to register
class TestYdbDiscoveryServer
{
private:
    TFakeYdbDiscoveryService Service;
    std::unique_ptr<grpc::Server> Server;

    TPortManager PortManager;
    ui16 Port = 0;

public:
    TestYdbDiscoveryServer(ui64 dynamicNodeIcPort)
        : Service(dynamicNodeIcPort)
    {}

    ~TestYdbDiscoveryServer()
    {
        Stop();
    }

    void Start()
    {
        grpc::ServerBuilder builder;
        builder.RegisterService(&Service);

        Port = PortManager.GetPort();
        const auto endpoint = "[::]:" + IntToString<10>(Port);
        builder.AddListeningPort(endpoint, MakeSslServerCredentials());

        Server = builder.BuildAndStart();
        UNIT_ASSERT(Server);
    }

    void Stop()
    {
        if (Server) {
            Server->Shutdown();
            Server.reset();
        }
    }

    ui16 GetPort() const
    {
        return Port;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBootstrapVhostTest)
{
    Y_UNIT_TEST(ShouldNotWaitForRequestsWhenNodeLeaseIsExpired)
    {
        TPortManager PortManager;
        const auto staticNodeIcPort = PortManager.GetPort();
        const auto dynamicNodeIcPort = PortManager.GetPort();

        TestYdbDiscoveryServer ydbDiscoveryServer(dynamicNodeIcPort);
        ydbDiscoveryServer.Start();

        auto moduleFactories = std::make_shared<NKikimr::TModuleFactories>();

        auto vhostFactories =
            std::make_shared<NDaemon::TVhostModuleFactories>();
        vhostFactories->LoopFactory = NFuse::CreateFuseLoopFactory;

        TBootstrapVhost bootstrap(
            std::move(moduleFactories),
            std::move(vhostFactories));

        TTempDir dir;

        auto sysConfigPath = dir.Path() / "nfs-sys.txt";
        TOFStream(sysConfigPath.GetPath()).Write(SysConfigStr);

        auto storageConfigPath = dir.Path() / "nfs-storage.txt";
        auto storageConfigStr = Sprintf(
            StorageConfigTemplateStr.c_str(),
            GetCertFile().c_str(),      // NodeRegistrationRootCertsFile
            GetCertFile().c_str(),      // NodeRegistrationCert.CertFile
            GetCertKeyFile().c_str()    // NodeRegistrationCert.CertPrivateKeyFile
        );
        TOFStream(storageConfigPath.GetPath()).Write(storageConfigStr);

        auto namingConfigPath = dir.Path() / "nfs-names.txt";
        auto namingConfigStr = Sprintf(
            NamingConfigTemplateStr.c_str(),
            staticNodeIcPort);
        TOFStream(namingConfigPath.GetPath()).Write(namingConfigStr);

        auto domainsConfigPath = dir.Path() / "nfs-domains.txt";
        TOFStream(domainsConfigPath.GetPath()).Write(DomainsConfigStr);

        TVector<TString> args = {
            "./program",

            "--service", "kikimr",

            "--sys-file", sysConfigPath,
            "--storage-file", storageConfigPath,
            "--naming-file", namingConfigPath,
            "--domains-file", domainsConfigPath,

            "--ic-port", IntToString<10>(dynamicNodeIcPort),

            "--domain", "local",

            "--node-broker", "localhost",
            "--node-broker-port", IntToString<10>(ydbDiscoveryServer.GetPort()),

            "--use-secure-registration",
        };

        TVector<char*> argv;
        argv.reserve(args.size());

        for (auto& arg : args) {
            argv.push_back(arg.begin());
        }

        bootstrap.ParseOptions(argv.size(), argv.data());

        bootstrap.Init();
        bootstrap.Start();

        // Emulate NodeBroker lease expiration - it is simpler than creating real
        // conditions for it
        //
        // The correspondence between node lease expiration and
        // |NodeLeaseExpirationExitCode| is verified in the
        // ShouldObserveSpecificReturnCodeWhenNodeLeaseIsExpired test
        bootstrap.GetShouldContinue().ShouldStop(NodeLeaseExpirationExitCode);

        UNIT_ASSERT_EXCEPTION(
            bootstrap.Stop(),
            TAppShouldExitWithoutShutdownException);

        // This looks hacky, but we need TBootstrap::Stop to finish without
        // exceptions. Otherwise, Actor System threads will not be joined and
        // the test will crash
        //
        // In the production binary, the behavior is different: when TApp
        // receives TAppShouldExitWithoutShutdownException, it calls _exit and
        // terminates the process immediately
        bootstrap.GetShouldContinue().ShouldStop(0);
        bootstrap.Stop();
    }

    // This test asserts that |NodeLeaseExpirationExitCode| has the expected
    // value and correctly corresponds to NodeBroker lease expiration
    Y_UNIT_TEST(ShouldObserveSpecificReturnCodeWhenNodeLeaseIsExpired)
    {
        NStorage::TTestEnv env({});
        auto& runtime = env.GetRuntime();

        ui32 nodeIdx = env.AddDynamicNode();

        auto& appData = runtime.GetAppData(nodeIdx);

        // If KikimrShouldContinue is not nullptr, this code is not needed and
        // can be removed
        UNIT_ASSERT(!appData.KikimrShouldContinue);
        TProgramShouldContinue shouldContinue;
        // HACK(svartmetal): KikimrShouldContinue is a const pointer, so
        // const_cast is needed to modify it
        const_cast<TProgramShouldContinue*&>(appData.KikimrShouldContinue) =
            &shouldContinue;
        Y_DEFER {
            const_cast<TProgramShouldContinue*&>(appData.KikimrShouldContinue) =
                nullptr;
        };

        const TDuration leaseExpirationTimeout = TDuration::Hours(2);

        IActorPtr leaseHolder(NNodeBroker::CreateLeaseHolder(
            runtime.GetCurrentTime() + leaseExpirationTimeout));
        auto leaseHolderId = runtime.Register(
            leaseHolder.release(),
            nodeIdx,
            appData.UserPoolId,
            TMailboxType::Simple,
            0);
        runtime.EnableScheduleForActor(leaseHolderId);
        runtime.RegisterService(TActorId(), leaseHolderId, 0);

        // Advance time to trigger NodeBroker lease expiration
        runtime.AdvanceCurrentTime(leaseExpirationTimeout);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        // Expect that lease is expired
        UNIT_ASSERT(
            TProgramShouldContinue::Stop == shouldContinue.PollState());
        UNIT_ASSERT_VALUES_EQUAL(
            NodeLeaseExpirationExitCode,
            shouldContinue.GetReturnCode());
    }
};

}   // namespace NCloud::NFileStore::NDaemon
