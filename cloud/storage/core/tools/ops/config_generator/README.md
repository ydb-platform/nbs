# config generator

### Usage
1. Edit configs in cloud/nbs_internal/storage/core/tools/ops/config_generator/services/{nbs/nfs}
2. Build config_generator
3. Run generate
```bash
./config_generator -a {ARCADIA_PATH} -s {ARCADIA_PATH}/cloud/nbs_internal/storage/core/tools/ops/config_generator/services/{nbs/nfs}
```

# Mac

For mac builds also pass `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk`, for some reason 27.0 doesn't work

Download it (if it is not installed already):

```
wget https://download.developer.apple.com/Developer_Tools/Command_Line_Tools_26.5/Command_Line_Tools_26.5_Universal.dmg
```

If direct download is not available you can download it from https://developer.apple.com/download/all/?q=26.5

```bash
export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk
ya make -DSDKROOT="${SDKROOT}" --host-platform-flag="SDKROOT=${SDKROOT}" cloud/storage/core/tools/ops/config_generator/
./config_generator -a {ARCADIA_PATH} -s {ARCADIA_PATH}/cloud/storage/core/tools/ops/config_generator/services/{nbs/nfs}
```
