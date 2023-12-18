# XFS test suite

## Description
This runs xfs test suite on filestore fs in selected cluster.

## Actions sequence
1. Create instance (via `ycp`).
2. Create test filesystem (via `ycp`) and attach it to instance.
3. Create scratch filesystem (via `ycp`) and attach it to instance.
4. Instal necessary packages.
5. Clone and setup git://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git repository.
6. Setup local.config
7. Create test user.
8. Run xfs test suite.

## Usage
Install `ycp` and reload shell:
```(bash)
$ curl https://s3.mds.yandex.net/mcdev/ycp/install.sh | bash
$ cat > ~/.bashrc <<EOF
PATH="$HOME/ycp/bin/:$PATH"
EOF
```

Patch `/etc/hosts` to be able to use `ycp` with `hw-nbs-stable-lab`:
```(bash)
$ export HW_NBS_STABLE_LAB_SEED_IP=$(host $(pssh list C@cloud_hw-nbs-stable-lab_seed) | awk '{print $5}')
$ echo -e "\n# ycp hack for hw-nbs-stable-lab\n$HW_NBS_STABLE_LAB_SEED_IP local-lb.cloud-lab.yandex.net" | sudo tee -a /etc/hosts
```

Create `/etc/ssl/certs/hw-nbs-stable-lab.pem` with the content https://paste.yandex-team.ru/3966169

Save private ssh key from https://yav.yandex-team.ru/secret/sec-01ehpt7c9ez5g4j9nx4g4yegj3/explore/version/ver-01ehpt7c9ng1t28517aqjpavd5 to ~/.ssh/overlay, then execute:

```(bash)
$ ssh-add ~/.ssh/overlay`
```

Build `yc-nfs-ci-xfs-test-suite`:
```(bash)
$ ya make
```

## Example
```(bash)
$ YAV_TOKEN=<your_oauth_yav_token> ./yc-nfs-ci-xfs-test-suite --cluster preprod --test-type virtiofs --test-device nfs-test-dev --test-dir /mnt/test --scratch-type virtiofs --scratch-device nfs-scratch-dev --scratch-dir /mnt/scratch --script-name default.sh
```

You can find how to get OAuth YAV_TOKEN in [this instruction](https://docs.yandex-team.ru/yav-api/).
