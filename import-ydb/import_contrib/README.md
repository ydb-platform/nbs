# Patch ydb source code

```
GIT_YDB=$GIT_ROOT/ydb
source $SCRIPT_DIR/patch_cmakes.sh $GIT_YDB
source $SCRIPT_DIR/patch_ymakes.sh $GIT_YDB
source $SCRIPT_DIR/patch_sources.sh $GIT_YDB
source $SCRIPT_DIR/patch_py_sources.sh $GIT_YDB
source $SCRIPT_DIR/patch_protos.sh $GIT_YDB
source $SCRIPT_DIR/patch_configs.sh $GIT_YDB
```
