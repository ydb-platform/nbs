
if [ -z ${CONFIGURED} ]; then
export CONFIGURED=1

[ -z "${VERBOSE=${V}}" ] && set +x
[ -n "${VERBOSE=${V}}" ] && set -x
[ -z "${KEEP}" ]         && set -e

#export ARCADIA_ROOT=${ARCADIA_ROOT:=`ya dump root || echo ~/arcadia/`}

ROOT_DIR=$(git rev-parse --show-toplevel)


# CUR_DIR=${CUR_DIR:=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

# YA_MAKE_ADD=--sanitize=address
# YA_MAKE_ADD=--sanitize=memory
# YA_MAKE_ADD=--sanitize=thread
# YA_MAKE_ADD=--sanitize=undefined
# YA_MAKE_ADD=--sanitize=leak
# YA_MAKE_ADD=-r

#YA_MAKE_ADD=${YA_MAKE_ADD=-r}
#YA_MAKE=${YA_MAKE="ya make ${YA_MAKE_ADD}"} # "

REBUILD=${REBUILD=1}

# STARTER='ya tool gdb -ex run --args'
# STARTER='ya tool gdb -ex run -ex backtrace --args'
# STARTER='GRPC_VERBOSITY=info GRPC_TRACE=tcp,http,api'




function re_create_mount {
pushd ${ROOT_DIR}
ya make -r cloud/filestore/buildall
popd

pushd ${ROOT_DIR}/cloud/filestore/bin

. initctl.sh
umount ${MOUNT_POINT} ||:
. initctl.sh stop

. initctl.sh format initialize
. initctl.sh create startendpoint
. initctl.sh start
. initctl.sh mount

sleep 3
echo mountpoint = ${MOUNT_POINT}
popd

}


# === Library section

# Add pid of last runned program to "kill on exit" list
function last_killer {
    [ -n "${NO_KILL}" ] && NO_KILL_ECHO="echo "
    TRAP+="${NO_KILL_ECHO} kill $! ||:;"
    trap "$TRAP wait;" EXIT SIGINT SIGQUIT SIGTERM
}

# Run program as daemon. Kill at exit
function daemon_killer {
    bash -c "eval $* " &
    last_killer
}

function on_exit {
    TRAP+="$* ||:;"
    trap "$TRAP" EXIT SIGINT SIGQUIT SIGTERM
}

function tmpl {
    cp -v $1.tmpl $1
    perl -p -i -E 's/\${(.+)}/$ENV{$1}/g' $1
}

fi
