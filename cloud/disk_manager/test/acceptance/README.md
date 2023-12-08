# Acceptance test

## Description
1. Runs main test:
    1. Creates snapshot from the disk (via `ycp`), then creates incremental snapshot and disk from this snapshot (via `ycp`).
    2. Creates pooled image from snapshot (via `ycp`), then creates disk from this image (via `ycp`).
    3. Creates image from disk, created on step 1.1 .(via `ycp`), then creates image from this image (via `ycp`).
2. Runs cancel test:
    1. Asynchronously creates snapshot for every disk (via `ycp`).
    2. Tries to delete snapshot while it is in CREATING status (via `ycp`).
    3. Checks that snapshot was successfully deleted (via `ycp`).

## Usage
Install `ycp` and reload shell:
```(bash)
$ curl https://s3.mds.yandex.net/mcdev/ycp/install.sh | bash
$ cat > ~/.bashrc <<EOF
PATH="$HOME/ycp/bin/:$PATH"
EOF
```

Install `ycp` configuration for tests:
```(bash)
$ mkdir -p ~/.config/ycp/
$ mv ~/.config/ycp/config.yaml ~/.config/ycp/config.yaml.bak
$ cp ../../../packages/yc-nbs-ci-tools/ycp-config.yaml ~/.config/ycp/config.yaml
```

Patch `/etc/hosts` to be able to use `ycp` with `hw-nbs-stable-lab`:
```(bash)
$ export HW_NBS_STABLE_LAB_SEED_IP=$(host $(pssh list C@cloud_hw-nbs-stable-lab_seed) | awk '{print $5}')
$ echo -e "\n# ycp hack for hw-nbs-stable-lab\n$HW_NBS_STABLE_LAB_SEED_IP local-lb.cloud-lab.yandex.net" | sudo tee -a /etc/hosts
```

Create `/etc/ssl/certs/hw-nbs-stable-lab.pem` with the content https://paste.yandex-team.ru/3966169

Save private ssh key from https://yav.yandex-team.ru/secret/sec-01ehpt7c9ez5g4j9nx4g4yegj3/explore/version/ver-01ehpt7c9ng1t28517aqjpavd5 to ~/.ssh/overlay, then `ssh-add ~/.ssh/overlay`

Build `acceptance-test`:
```(bash)
$ ya make
```

Example:
```(bash)
$ ./acceptance --profile hw-nbs-stable-lab --folder-id yc.dogfood.serviceFolder --src-disk-ids <any_disk_id>,<another_disk_id> --verbose
```

## Leaked resources
There is a possibility when the test can leave leaked resources inside the cloud. So if it is happened:
1. Find all snapshots inside the cluster with the name pattern `acceptance-test-snapshot-<suffix>-<timestamp>` and delete them manually.
2. Find all images inside the cluster with the name pattern `acceptance-test-image-<suffix>-<timestamp>` and delete them manually.
3. Find all disks inside the cluster with the name pattern `acceptance-test-disk-<suffix>-<timestamp>` and delete them manually.
