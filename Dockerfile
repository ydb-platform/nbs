# syntax=docker/dockerfile:1
FROM ubuntu:22.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG THREADS=31
ARG GIT_BRANCH=master
ARG GIT_COMMIT=71d3b8854c772c529147fb200c3e407050bd41b0
ENV TZ=Etc/UTC

RUN <<EOF
    apt-get update
    apt-get install -y --no-install-recommends git wget gnupg lsb-release curl xz-utils tzdata cmake \
        python3-dev python3-pip ninja-build antlr3 m4 libidn11-dev libaio1 libaio-dev make clang-12 \
        lld-12 llvm-12 clang-14 lld-14 llvm-14 file
    pip3 install conan==1.59 grpcio-tools pyinstaller==5.13.2 six pyyaml packaging PyHamcrest cryptography
    (V=4.8.1; curl -L https://github.com/ccache/ccache/releases/download/v${V}/ccache-${V}-linux-x86_64.tar.xz | \
         tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-${V}-linux-x86_64/ccache)
    rm -rf /var/lib/apt/lists/*
EOF

RUN <<EOT
    mkdir /build && cd /build
    git init
    git remote add origin https://github.com/ydb-platform/nbs.git
    git fetch --depth=1 origin ${GIT_COMMIT}
    git checkout FETCH_HEAD
EOT
WORKDIR /build

RUN <<EOF
    export CONAN_USER_HOME=/build
    export CCACHE_BASEDIR=/
    export CCACHE_SLOPPINESS=locale
    export CCACHE_REMOTE_STORAGE="http://cachesrv.ydb.tech:8080|read-only|layout=bazel"
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
        -DCCACHE_PATH=/usr/local/bin/ccache \
        -DCMAKE_TOOLCHAIN_FILE=/build/clang.toolchain \
        /build
EOF
RUN ninja -j${THREADS} ./cloud/blockstore/apps/client/blockstore-client
RUN ninja -j${THREADS} ./cloud/blockstore/apps/server/nbsd
RUN ninja -j${THREADS} ./cloud/blockstore/tools/nbd/blockstore-nbd
RUN ninja -j${THREADS} ./cloud/blockstore/apps/disk_agent/diskagentd
RUN <<EOF
    ccache -s
    mv ./cloud/blockstore/apps/client/blockstore-client /
    mv ./cloud/blockstore/apps/server/nbsd /
    mv ./cloud/blockstore/tools/nbd/blockstore-nbd /
    mv ./cloud/blockstore/apps/disk_agent/diskagentd /
    rm -rf /build
EOF

FROM ubuntu:22.04 AS client
RUN apt-get update  \
    && apt-get install --no-install-recommends -y libidn12 libaio1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /blockstore-client /blockstore-client
RUN <<EOF
    ls -lsha /
EOF
CMD ["/blockstore-client"]

FROM ubuntu:22.04 AS server
RUN apt-get update  \
    && apt-get install --no-install-recommends -y libidn12 libaio1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /nbsd /nbsd
EXPOSE ${NBS_MON_PORT:-8766}
CMD ["/nbsd"]

FROM ubuntu:22.04 AS nbd
RUN apt-get update  \
    && apt-get install --no-install-recommends -y libidn12 libaio1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /blockstore-nbd /blockstore-nbd
CMD ["/blockstore-nbd"]

FROM ubuntu:22.04 AS diskagent
RUN apt-get update  \
    && apt-get install --no-install-recommends -y libidn12 libaio1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /diskagentd /diskagentd
EXPOSE ${DA_MON_PORT:-8772}
CMD ["/diskagentd"]