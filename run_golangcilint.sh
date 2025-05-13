#!/usr/bin/env bash

cd  "$(dirname "$0")" || exit 1

MERGE_BASE=$(git merge-base HEAD main)
GOLANG_FILES_CHANGED=$(git diff --name-only "${MERGE_BASE}" | grep -P '\.go$')
if [ -z "$GOLANG_FILES_CHANGED" ]; then
  echo "No Go files changed, skipping lint"
  exit 0
fi

BASE_DIRECTORY="/tmp/nbs-go-lint"
SOURCE_VOLUME="source-volume"
YA_VOLUME="ya-volume"

mkdir -p "${BASE_DIRECTORY}"
docker run --rm -v "${BASE_DIRECTORY}:/tmp/nbs-go-lint" bash "rm -rf /tmp/nbs-go-lint"
docker volume rm "${SOURCE_VOLUME}" "${YA_VOLUME}"
RUN_DIRECTORY=$(mktemp -d "${BASE_DIRECTORY}/tmp.XXXXXX")


function upper_dir(){
  echo "${RUN_DIRECTORY}/upper/$1"
}

function create_overlay_volume(){
  # Create an overlay volume which uses host directory as a base layer.
  name=$1
  source=$2
  VOLUME_WORK_DIR="${RUN_DIRECTORY}/work/${name}"
  VOLUME_UPPERDIR="$(upper_dir "${name}")"
  mkdir -p "${VOLUME_WORK_DIR}" "${VOLUME_UPPERDIR}"
  docker volume create --driver local --opt type=overlay \
    --opt "o=lowerdir=${source},upperdir=$(upper_dir "${name}"),workdir=${VOLUME_WORK_DIR}" \
    --opt device=overlay "${name}"
}

function docker_run_with_volumes(){
  WORK_DIR="/app"
  LOCAL_YA_PATH="/root/.ya"
  docker run \
  --tty \
  --rm \
  --volume "${SOURCE_VOLUME}:${WORK_DIR}" \
  --volume "${YA_VOLUME}:${LOCAL_YA_PATH}" \
  --workdir "${WORK_DIR}" \
  --env CGO_CFLAGS="-I${WORK_DIR}/contrib/libs/zstd/include" \
  --env  GOSUMDB=off \
  --env LOCAL_WORK_DIR="${WORK_DIR}" \
  --env UID="$(id -u)" \
  --env GID="$(id -g)" \
  "$@"
}

function run_lint() {
  LINTER_IMAGE="golangci/golangci-lint:v2.1.6"
  docker_run_with_volumes "${LINTER_IMAGE}" "$@"
}

function build_protos(){
  run_lint ./ya make --replace-result --add-result=.pb.go "$@"
}

create_overlay_volume "${SOURCE_VOLUME}" "$(pwd)"
create_overlay_volume "${YA_VOLUME}" "${HOME}/.ya"

result=()
while read -r dir; do
  result+=("${dir}")
done < <(find cloud/ -name '*.proto' | xargs dirname | sort | uniq)
result+=("cloud/disk_manager/test/mocks/")

build_protos "${result[@]}"
if [ $? -ne 0 ]; then
  echo "Protobuf generation failed"
  exit 1
fi

run_lint golangci-lint custom -v
if [ $? -ne 0 ]; then
  echo "Build of nbs-go-lint failed"
  exit 1
fi


function run_linter() {
  prefix=$1
  shift

  run_lint ./nbs-go-lint run -v "$@"
  run_lint bash -c 'chown "$(id -u):$(id -g)" ${LOCAL_WORK_DIR}/golangci-lint-report.*'
  exit_code=$?
  SOURCE_UPPER_DIR="$(upper_dir "${SOURCE_VOLUME}")"
  cp "${SOURCE_UPPER_DIR}/golangci-lint-report.html" "${prefix}-golangci-lint-report.html"
  cp "${SOURCE_UPPER_DIR}/golangci-lint-report.xml" "${prefix}-golangci-lint-report.xml"
  if [ $exit_code -ne 0 ]; then
    echo "Linter failed"
    exit 1
  fi
}

run_linter "total" "cloud/..."
run_linter "changed" "${GOLANG_FILES_CHANGED[@]}" || exit 1
