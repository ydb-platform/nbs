set -eux

: "${MOUNT_PATH:?}"
: "${LOG_PATH:?}"

TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"

cd "${MOUNT_PATH}"

if [ ! -d "postgresql" ]; then
    git clone --depth=1 --branch REL_16_STABLE https://github.com/postgres/postgres.git postgresql \
        >>"${LOG_PATH}" 2>&1
fi

cd postgresql

./configure --without-icu --without-readline --without-zlib >>"${LOG_PATH}" 2>&1

timeout "${TIMEOUT_SECONDS}s" bash -c '
    while true; do
        make -j4 clean >>"'"${LOG_PATH}"'" 2>&1 || true
        make -j4 >>"'"${LOG_PATH}"'" 2>&1
    done
'
