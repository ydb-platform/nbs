#include "version.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVersionTest)
{
    Y_UNIT_TEST(ShouldParseUrlFromFullInfoTrunk)
    {
        TString info = R"___(
        Svn info:
            URL: svn+ssh://zomb-sandbox-rw@arcadia.yandex.ru/arc/trunk/arcadia
            Last Changed Rev: 5106004
            Last Changed Author: robot-srch-releaser
            Last Changed Date: 2019-06-06 03:01:38 +0300 (Thu, 06 Jun 2019)

        Other info:
            Build by: sandbox
            Top src dir: /place/sandbox-data/srcdir/arcadia_cache
            Top build dir: /place/sandbox-data/build_cache/yabuild/build/build_root/ytx4/000ad7
            Hostname: linux-ubuntu-12-04-precise
            Host information:
                Linux linux-ubuntu-12-04-precise 4.9.151-35 #1 SMP Thu Jan 17 16:21:25 UTC 2019 x86_64 x86_64 x86_64 GNU/Linux

        Build info:
            Compiler: /place/sandbox-data/build_cache/tools/v3/958916803/bin/clang++
            Compiler version:
                clang version 7.0.0 (tags/RELEASE_700/final)
                Target: x86_64-unknown-linux-gnu
                Thread model: posix
                InstalledDir: /place/sandbox-data/build_cache/tools/v3/958916803/bin
            Compile flags: -pipe -m64 -O3 -g -ggnu-pubnames -fexceptions -W -Wall -Wno-parentheses -DFAKEID=5020880 -DARCADIA_ROOT=/place/sandbox-data/srcdir/arcadia_cache -DARCADIA_BUILD_ROOT=/place/sandbox-data/build_cache/yabuild/build/build_root/ytx4/000ad0 -D_THREAD_SAFE -D_PTHREADS -D_REENTRANT -D_LIBCPP_ENABLE_CXX17_REMOVED_FEATURES -D_LARGEFILE_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -DGNU -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DNDEBUG -D__LONG_LONG_SUPPORTED -DSSE_ENABLED=1 -DSSE3_ENABLED=1 -DSSSE3_ENABLED=1 -ggdb -fno-omit-frame-pointer -DNALF_FORCE_MALLOC_FREE -nostdinc++ -msse2 -msse3 -mssse3 -Werror -std=c++1z -Woverloaded-virtual -Wno-invalid-offsetof -Wno-attributes -Wno-dynamic-exception-spec -Wno-register -Wimport-preprocessor-directive-pedantic -Wno-c++17-extensions -Wno-exceptions -Wno-inconsistent-missing-override -Wno-undefined-var-template -Wno-return-std-move -nostdinc++
        )___";

        auto url = GetSvnUrlFromInfo(info);
        UNIT_ASSERT_EQUAL(
            url,
            "svn+ssh://zomb-sandbox-rw@arcadia.yandex.ru/arc/trunk/arcadia");
    }

    Y_UNIT_TEST(ShouldParseUrlFromFullInfoStable192)
    {
        TString info = R"___(
        Svn info:
            URL: svn+ssh://arcadia.yandex.ru/arc/branches/kikimr/stable-19-2/arcadia
            Last Changed Rev: 5079915
            Last Changed Author: davenger
            Last Changed Date: 2019-05-28 19:34:24 +0300 (Tue, 28 May 2019)

        Other info:
            Build by: sandbox
            Top src dir: /place/sandbox-data/tasks/0/0/438590800/arcadia
            Top build dir: /place/sandbox-data/build_cache/yabuild/build/build_root/u6su/000a6c
            Hostname: linux-ubuntu-12-04-precise
            Host information:
                Linux linux-ubuntu-12-04-precise 4.9.151-35 #1 SMP Thu Jan 17 16:21:25 UTC 2019 x86_64 x86_64 x86_64 GNU/Linux

        Build info:
            Compiler: /place/sandbox-data/build_cache/tools/v3/707372498/bin/clang++
            Compiler version:
                clang version 7.0.0 (tags/RELEASE_700/final)
                Target: x86_64-unknown-linux-gnu
                Thread model: posix
                InstalledDir: /place/sandbox-data/build_cache/tools/v3/707372498/bin
            Compile flags: -pipe -m64 -mssse3 -msse -msse3 -msse2 -O3 -g -ggnu-pubnames -fexceptions -W -Wall -Wno-parentheses -DFAKEID=4286128 -DARCADIA_ROOT=/place/sandbox-data/tasks/0/0/438590800/arcadia -DARCADIA_BUILD_ROOT=/place/sandbox-data/build_cache/yabuild/build/build_root/u6su/000a65 -D_THREAD_SAFE -D_PTHREADS -D_REENTRANT -D_LIBCPP_ENABLE_CXX17_REMOVED_FEATURES -D_LARGEFILE_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -DGNU -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DSSSE3_ENABLED=1 -DSSE_ENABLED=1 -DSSE3_ENABLED=1 -DSSE2_ENABLED=1 -DNDEBUG -D__LONG_LONG_SUPPORTED -ggdb -fno-omit-frame-pointer -DNALF_FORCE_MALLOC_FREE -nostdinc++ -Werror -std=c++1z -Woverloaded-virtual -Wno-invalid-offsetof -Wno-attributes -Wno-dynamic-exception-spec -Wno-register -Wimport-preprocessor-directive-pedantic -Wno-c++17-extensions -Wno-exceptions -Wno-inconsistent-missing-override -Wno-undefined-var-template -Wno-return-std-move -nostdinc++

        linked with malloc: lfalloc_yt
        )___";

        auto url = GetSvnUrlFromInfo(info);
        UNIT_ASSERT_EQUAL(
            url,
            "svn+ssh://arcadia.yandex.ru/arc/branches/kikimr/stable-19-2/"
            "arcadia");
    }

    Y_UNIT_TEST(ShouldParseUrlFromFullInfoTag)
    {
        TString info = R"___(
        Svn info:
            URL: svn+ssh://arcadia.yandex.ru/arc/tags/kikimr/stable-19-2-29/arcadia
            Last Changed Rev: 5079915
            Last Changed Author: davenger
            Last Changed Date: 2019-05-28 19:34:24 +0300 (Tue, 28 May 2019)

        Other info:
            Build by: sandbox
            Top src dir: /place/sandbox-data/tasks/0/0/438590800/arcadia
            Top build dir: /place/sandbox-data/build_cache/yabuild/build/build_root/u6su/000a6c
            Hostname: linux-ubuntu-12-04-precise
            Host information:
                Linux linux-ubuntu-12-04-precise 4.9.151-35 #1 SMP Thu Jan 17 16:21:25 UTC 2019 x86_64 x86_64 x86_64 GNU/Linux

        Build info:
            Compiler: /place/sandbox-data/build_cache/tools/v3/707372498/bin/clang++
            Compiler version:
                clang version 7.0.0 (tags/RELEASE_700/final)
                Target: x86_64-unknown-linux-gnu
                Thread model: posix
                InstalledDir: /place/sandbox-data/build_cache/tools/v3/707372498/bin
            Compile flags: -pipe -m64 -mssse3 -msse -msse3 -msse2 -O3 -g -ggnu-pubnames -fexceptions -W -Wall -Wno-parentheses -DFAKEID=4286128 -DARCADIA_ROOT=/place/sandbox-data/tasks/0/0/438590800/arcadia -DARCADIA_BUILD_ROOT=/place/sandbox-data/build_cache/yabuild/build/build_root/u6su/000a65 -D_THREAD_SAFE -D_PTHREADS -D_REENTRANT -D_LIBCPP_ENABLE_CXX17_REMOVED_FEATURES -D_LARGEFILE_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -DGNU -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DSSSE3_ENABLED=1 -DSSE_ENABLED=1 -DSSE3_ENABLED=1 -DSSE2_ENABLED=1 -DNDEBUG -D__LONG_LONG_SUPPORTED -ggdb -fno-omit-frame-pointer -DNALF_FORCE_MALLOC_FREE -nostdinc++ -Werror -std=c++1z -Woverloaded-virtual -Wno-invalid-offsetof -Wno-attributes -Wno-dynamic-exception-spec -Wno-register -Wimport-preprocessor-directive-pedantic -Wno-c++17-extensions -Wno-exceptions -Wno-inconsistent-missing-override -Wno-undefined-var-template -Wno-return-std-move -nostdinc++

        linked with malloc: lfalloc_yt
        )___";

        auto url = GetSvnUrlFromInfo(info);
        UNIT_ASSERT_EQUAL(
            url,
            "svn+ssh://arcadia.yandex.ru/arc/tags/kikimr/stable-19-2-29/"
            "arcadia");
    }

    Y_UNIT_TEST(ShouldParseUrlFromFullInfoWithNbsTag)
    {
        TString info = R"___(
        Svn info:
            URL: svn+ssh://arcadia.yandex.ru/arc/tags/nbs/stable-19-4-44/arcadia
            Last Changed Rev: 5844870
            Last Changed Author: qkrorlqr
            Last Changed Date: 2019-10-21 17:06:02 +0300 (Mon, 21 Oct 2019)

        Other info:
            Build by: sandbox
            Top src dir: /place/sandbox-data/tasks/1/7/532847571/arcadia
            Top build dir: /place/sandbox-data/build_cache/yabuild/build/build_root/04xl/000b25
            Hostname: linux-ubuntu-12-04-precise
            Host information:
                Linux linux-ubuntu-12-04-precise 4.9.151-35 #1 SMP Thu Jan 17 16:21:25 UTC 2019 x86_64 x86_64 x86_64 GNU/Linux

        Build info:
            Compiler: /place/sandbox-data/build_cache/tools/v3/707372498/bin/clang++
            Compiler version:
                clang version 7.0.0 (tags/RELEASE_700/final)
                Target: x86_64-unknown-linux-gnu
                Thread model: posix
                InstalledDir: /place/sandbox-data/build_cache/tools/v3/707372498/bin
            Compile flags: -pipe -m64 -O3 -g -ggnu-pubnames -fexceptions -W -Wall -Wno-parentheses -DFAKEID=4659992 -DARCADIA_ROOT=/place/sandbox-data/tasks/1/7/532847571/arcadia -DARCADIA_BUILD_ROOT=/place/sandbox-data/build_cache/yabuild/build/build_root/04xl/000b1e -D_THREAD_SAFE -D_PTHREADS -D_REENTRANT -D_LIBCPP_ENABLE_CXX17_REMOVED_FEATURES -D_LARGEFILE_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -DGNU -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DNDEBUG -D__LONG_LONG_SUPPORTED -DSSE_ENABLED=1 -DSSE3_ENABLED=1 -DSSSE3_ENABLED=1 -ggdb -fno-omit-frame-pointer -DNALF_FORCE_MALLOC_FREE -nostdinc++ -msse2 -msse3 -mssse3 -Werror -std=c++1z -Woverloaded-virtual -Wno-invalid-offsetof -Wno-attributes -Wno-dynamic-exception-spec -Wno-register -Wimport-preprocessor-directive-pedantic -Wno-c++17-extensions -Wno-exceptions -Wno-inconsistent-missing-override -Wno-undefined-var-template -Wno-return-std-move -nostdinc++
        )___";

        auto url = GetSvnUrlFromInfo(info);
        UNIT_ASSERT_EQUAL(
            url,
            "svn+ssh://arcadia.yandex.ru/arc/tags/nbs/stable-19-4-44/arcadia");
    }

    Y_UNIT_TEST(ShouldParseBranchFromUrlTrunk)
    {
        TString url =
            "svn+ssh://zomb-sandbox-rw@arcadia.yandex.ru/arc/trunk/arcadia";
        auto branch = GetSvnBranchFromUrl(url);
        UNIT_ASSERT_VALUES_EQUAL(branch, "trunk");
    }

    Y_UNIT_TEST(ShouldParseBranchFromUrlStable192)
    {
        TString url =
            "svn+ssh://arcadia.yandex.ru/arc/branches/kikimr/stable-19-2/"
            "arcadia";
        auto branch = GetSvnBranchFromUrl(url);
        UNIT_ASSERT_VALUES_EQUAL(branch, "stable-19-2");
    }

    Y_UNIT_TEST(ShoudlParseBranchFromUrlTag)
    {
        TString url =
            "svn+ssh://arcadia.yandex.ru/arc/tags/kikimr/stable-19-2-29/"
            "arcadia";
        auto branch = GetSvnBranchFromUrl(url);
        UNIT_ASSERT_VALUES_EQUAL(branch, "stable-19-2-29");
    }

    Y_UNIT_TEST(ShoudlParseBranchFromUrlWithNbsTag)
    {
        TString url =
            "svn+ssh://arcadia.yandex.ru/arc/tags/nbs/stable-19-4-44/arcadia";
        auto branch = GetSvnBranchFromUrl(url);
        UNIT_ASSERT_VALUES_EQUAL(branch, "stable-19-4-44");
    }

    Y_UNIT_TEST(ShouldParseRevisionFromBranch)
    {
        {
            TString branch = "stable-20-12-345-6";
            auto revision = GetRevisionFromBranch(branch);
            UNIT_ASSERT_VALUES_EQUAL(20123456, revision);
        }

        {
            TString branch = "stable-20-54-321";
            auto revision = GetRevisionFromBranch(branch);
            UNIT_ASSERT_VALUES_EQUAL(20543210, revision);
        }

        {
            TString branch = "stable-20-4-5-1";
            auto revision = GetRevisionFromBranch(branch);
            UNIT_ASSERT_VALUES_EQUAL(20040051, revision);
        }

        {
            TString branch = "stable-20-4-23";
            auto revision = GetRevisionFromBranch(branch);
            UNIT_ASSERT_VALUES_EQUAL(20040230, revision);
        }
    }

    Y_UNIT_TEST(ShouldNotParseRevisionFromInvalidBranch)
    {
        {
            TString branch = "unstable-20-12-345-6";
            auto revision = GetRevisionFromBranch(branch);
            UNIT_ASSERT_VALUES_EQUAL(-1, revision);
        }

        {
            TString branch = "stable-20-4";
            auto revision = GetRevisionFromBranch(branch);
            UNIT_ASSERT_VALUES_EQUAL(-1, revision);
        }

        {
            TString branch = "stable-20-4-5-12";
            auto revision = GetRevisionFromBranch(branch);
            UNIT_ASSERT_VALUES_EQUAL(-1, revision);
        }

        {
            TString branch = "stable-20-4-5-6-7";
            auto revision = GetRevisionFromBranch(branch);
            UNIT_ASSERT_VALUES_EQUAL(-1, revision);
        }
    }

    Y_UNIT_TEST(ShouldGetFullVersionString)
    {
        auto full = GetFullVersionString();
        UNIT_ASSERT_VALUES_UNEQUAL("", full);
    }
}

}   // namespace NCloud
