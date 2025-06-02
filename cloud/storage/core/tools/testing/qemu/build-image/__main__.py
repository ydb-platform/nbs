import argparse
import os
import subprocess
import yaml
from urllib.request import urlretrieve
import tempfile


def prepare_img(args):
    if args.image is None:
        args.image = "{release}-server-cloudimg-{arch}.img".format(**vars(args))
        img_url = ("{cloud_images_mirror}/{release}/current/"
                   "{release}-server-cloudimg-{arch}.img".format(**vars(args)))

        print("Downloading", img_url, end='', flush=True)

        def progress(blocknum, bs, size):
            if blocknum * bs % (10 << 20) < bs:
                print('.', end='', flush=True)

        urlretrieve(img_url, filename=args.image, reporthook=progress)
        print("done", flush=True)

    if args.no_resize:
        subprocess.check_call("cp {} {}".format(args.image, args.out))
    else:
        print("Resizing...", end='', flush=True)
        # convert it to raw to do actual resize
        subprocess.check_call("qemu-img convert -O raw {image} {out}.tmp".format(image=args.image, out=args.out), shell=True)
        subprocess.check_call("truncate -s 10G {out}.tmp".format(out=args.out), shell=True)
        # lame till 3.5+ parted couldn't fix gpt table in script mode
        # in turn sfdisk couldn't resize partitions
        subprocess.check_call("sfdisk --part-label {out}.tmp 1 lame".format(out=args.out), shell=True)
        subprocess.check_call("parted -s {out}.tmp resizepart 1 '100%'".format(out=args.out), shell=True)
        # and now get it back
        subprocess.check_call("qemu-img convert -O qcow2 {out}.tmp {out}".format(out=args.out), shell=True)
        subprocess.check_call("rm {out}.tmp".format(out=args.out), shell=True)

    print("done", flush=True)


def write_meta_data(filename):
    meta_data = {
        'instance-id': 'ubuntu',
        'local-hostname': 'ubuntu'
    }

    with open(filename, 'w') as f:
        f.write(yaml.dump(meta_data))


def ssh_pubkey_blob(ssh_privkey):
    # use pipe to make it work regardless of the permissions on the key file
    privkey_blob = open(ssh_privkey).read()
    proc = subprocess.run(["ssh-keygen", "-y", "-f", "/dev/fd/0"],
                          input=privkey_blob, stdout=subprocess.PIPE,
                          check=True, universal_newlines=True)
    return proc.stdout


def write_user_data(filename, args):
    pubkey_blob = ssh_pubkey_blob(args.ssh_key)

    user_data = {
        'users': [
            {
                'name': args.user,
                'lock_passwd': False,
                'plain_text_passwd': 'qemupass' if args.plain_pwd else None,
                'ssh_pwauth': args.plain_pwd,
                'ssh_authorized_keys': [pubkey_blob],
                'sudo': ['ALL=(ALL) NOPASSWD:ALL'],
                'shell': '/bin/bash',
            },
        ],
        'apt': {
            'primary': [
                {
                    'arches': ['default'],
                    'uri': args.repo_mirror,
                },
            ],
        },
        'packages': [
            'acl',
            'btop',
            'fio',
            'iotop',
            'mc',
            'mysql-server',
            'nfs-common',
            'postgresql',
            'sysbench',
            'tmux',
        ],
        'power_state': {
            'delay': 'now',
            'mode': 'poweroff',
        },
        'write_files': [],
        'runcmd' : [],
    }

    if args.release not in ["focal", "bionic"]:
        # There are several types of keys and signature algorithms
        # in the SSH protocol. RSA keys, which have the key type ssh-rsa,
        # can be used to sign with SHA-1 (in which case, the signature type
        # is ssh-rsa), SHA-256 (which has signature type rsa-sha2-256),
        # or SHA-512 (which has signature type rsa-sha2-512).
        # We're connecting with an RSA key and using the ssh-rsa signature type
        # with SHA-1. Unfortunately, SHA-1 is no longer secure, and the server
        # is telling you that it won't accept that signature type. In this case
        # we need to add this string to sshd_config to use this signature
        # algorithm.
        # this option is only supported since ubuntu 22.04
        user_data['write_files'].append({
            'path': '/etc/ssh/sshd_config',
            'content': 'PubkeyAcceptedAlgorithms +ssh-rsa',
            'append': True,
        })

    if args.plain_pwd:
        user_data['runcmd'].append(
            "echo '### use login: %s password: qemupass' >> /etc/issue" % args.user,
        )

    if args.with_rdma:
        user_data['runcmd'].extend([
            "apt install -y linux-modules-extra-`uname -r` rdma-core ibverbs-utils perftest",
            "bash -c 'echo rdma_rxe >> /etc/modules'",
        ])

    with open(filename, 'w') as f:
        f.write('#cloud-config\n' + yaml.dump(user_data))


def write_network_config(filename):
    # Minimal basic configuration.  Without it the guest would try to rename
    # the interface to the name from cloud-init time, which races with DHCP
    # client and may result in non-operational network.
    network_config = {
        'version': 2,
        'ethernets': {
            'eth0': {
                'dhcp4': True,
                'match': {
                    'name': '*'
                }
            }
        }
    }

    with open(filename, 'w') as f:
        f.write(yaml.dump(network_config))


def mk_cidata_iso(args, tmpdir):
    meta_data = os.path.join(tmpdir, 'meta-data')
    write_meta_data(meta_data)
    user_data = os.path.join(tmpdir, 'user-data')
    write_user_data(user_data, args)
    network_config = os.path.join(tmpdir, 'network-config')
    write_network_config(network_config)

    cidata_iso = os.path.join(tmpdir, 'cidata.iso')
    subprocess.check_call(['genisoimage', '-V', 'CIDATA', '-R',
                           '-o', cidata_iso,
                           meta_data, user_data, network_config])

    return cidata_iso


def qemu_bin(args):
    deb_arch_map = {
        'amd64': 'x86_64',
        'arm64': 'aarch64',
    }

    return "qemu-system-{}".format(deb_arch_map[args.arch])


def customize(args, cidata_iso):
    # arbitrary, should be enough
    nproc = 2
    mem = '2G'

    cmd = [
        qemu_bin(args),
        "-nodefaults",
        "-smp", str(nproc),
        "-m", mem,
        "-netdev", "user,id=netdev0",
        "-device", "virtio-net-pci,netdev=netdev0,id=net0",
        "-nographic",
        "-drive", "format=qcow2,file={},id=hdd0,if=none,aio=native,cache=none".format(args.out),
        "-device", "virtio-blk-pci,id=vblk0,drive=hdd0,num-queues={},bootindex=1".format(nproc),
        "-drive", "format=raw,file={},id=cidata,if=none,readonly=on".format(cidata_iso),
        "-device", "virtio-blk-pci,id=vblk1,drive=cidata",
        "-serial", "stdio",
    ]

    if args.arch == 'amd64':
        cmd += [
            "-accel", "kvm",
            "-cpu", "host",
            "-smbios", "type=1,serial=ds=nocloud",
        ]

    if args.arch == 'arm64':
        cmd += [
            "-machine", "virt",
            "-bios", "/usr/share/qemu-efi-aarch64/QEMU_EFI.fd",
            "-cpu", "cortex-a72",
        ]

    subprocess.check_call(cmd, timeout=25 * 60)


def main(args):
    prepare_img(args)

    with tempfile.TemporaryDirectory() as tmpdir:
        cidata_iso = mk_cidata_iso(args, tmpdir)

        customize(args, cidata_iso)

    print("\n{} is ready to use. See README.md for further instructions if necessary\n".format(args.out))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", help="output file path", default="rootfs.img")
    parser.add_argument("--release", help="Ubuntu release name",
                        default="noble")
    parser.add_argument("--arch", help="Ubuntu architecture", default="amd64")
    parser.add_argument("--image", help="image file path to use instead of downloading", default=None)
    parser.add_argument("--cloud-images-mirror",
                        help="http://cloud-images.ubuntu.com mirror",
                        default="http://mirror.yandex.ru/ubuntu-cloud-images")
    parser.add_argument("--repo-mirror", help="Ubuntu repository mirror",
                        default="http://mirror.yandex.ru/ubuntu")
    parser.add_argument("--user", help="guest user", default="qemu")
    parser.add_argument("--ssh-key", help="private ssh key for guest user",
                        required=True)
    parser.add_argument("--plain-pwd", help="Use password for user login", action='store_true')
    parser.add_argument("--no-resize", help="do not attempt to resize partitions", action='store_true')
    parser.add_argument("--with-rdma", help="Install rdma packages", action='store_true')

    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
