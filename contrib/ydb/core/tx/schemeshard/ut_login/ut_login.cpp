#include <contrib/ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <contrib/ydb/library/login/login.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardLoginTest) {
    Y_UNIT_TEST(BasicLogin) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        TActorId sender = runtime.AllocateEdgeActor();
        std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> transaction(CreateAlterLoginCreateUser(++txId, "user1", "password1"));
        transaction->Record.MutableTransaction(0)->SetWorkingDir("/MyRoot");
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, transaction.release());
        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        UNIT_ASSERT(describe.HasPathDescription());
        UNIT_ASSERT(describe.GetPathDescription().HasDomainDescription());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().HasSecurityState());
        UNIT_ASSERT(describe.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize() > 0);

        // check token
        NLogin::TLoginProvider login;
        login.UpdateSecurityState(describe.GetPathDescription().GetDomainDescription().GetSecurityState());
        auto resultValidate = login.ValidateToken({.Token = resultLogin.token()});
        UNIT_ASSERT_VALUES_EQUAL(resultValidate.User, "user1");
    }
}
