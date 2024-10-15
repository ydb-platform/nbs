#pragma once

namespace NCloud::NFileStore::NUnsensitivifier {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

void ConfigureSignals();

int AppMain(TBootstrap& bootstrap);
void AppStop(int exitCode);

}   // namespace NCloud::NFileStore::NUnsensitivifier
