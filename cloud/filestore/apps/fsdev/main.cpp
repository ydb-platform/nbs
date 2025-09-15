#include <util/system/compiler.h>

#include <cloud/filestore/libs/spdk/impl/env.h>

int main(int argc, char** argv)
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);

    NCloud::NFileStore::NSpdk::TSpdkEnvConfigPtr config;
    NCloud::NFileStore::NSpdk::CreateEnv(config);
    return 0;
}
