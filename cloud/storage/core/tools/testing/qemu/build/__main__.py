#!/usr/bin/env python

from __future__ import print_function

import argparse
import contextlib
import logging
import os
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile


@contextlib.contextmanager
def tmpdir(**kwargs):
    tmp = tempfile.mkdtemp(**kwargs)
    try:
        yield tmp
    finally:
        shutil.rmtree(tmp)


QEMU_CONFIG = [
    '--static',
    '--prefix=/usr',
    '--extra-cflags=-O3 -fno-semantic-interposition -falign-functions=32 -D_FORTIFY_SOURCE=2 -fPIE -Wno-maybe-uninitialized -Wno-array-bounds -Wno-deprecated-declarations -Wno-stringop-overflow',
    '--extra-ldflags=-z noexecstack -z relro -z now',
    '--audio-drv-list=',
    '--enable-attr',
    '--enable-cap-ng',
    # '--enable-curses',
    '--enable-kvm',
    '--enable-linux-aio',
    '--enable-numa',
    '--enable-tcg',
    '--enable-tools',
    '--enable-vhost-net',
    '--enable-virtfs',
    '--enable-vnc',
    '--enable-vnc-jpeg',
    '--enable-vvfat',
    '--disable-brlapi',
    '--disable-bzip2',
    '--disable-curl',
    '--disable-debug-tcg',
    '--disable-docs',
    '--disable-glusterfs',
    '--disable-gtk',
    '--disable-libiscsi',
    '--disable-libnfs',
    '--disable-libusb',
    '--disable-lzo',
    '--disable-opengl',
    '--disable-qom-cast-debug',
    '--disable-rdma',
    '--disable-sdl',
    '--disable-seccomp',
    '--disable-snappy',
    '--disable-spice',
    '--disable-tcg-interpreter',
    '--disable-usb-redir',
    '--disable-vnc-sasl',
    '--disable-vte',
    '--disable-xen',
]


QEMU_DEPS = [
    'autoconf',
    'automake',
    'bc',
    'bison',
    'cpio',
    'flex',
    'gawk',
    'git',
    'libaio-dev',
    'libaudit-dev',
    'libcap-dev',
    'libcap-ng-dev',
    'libdw-dev',
    'libelf-dev',
    'libglib2.0-0',
    'libglib2.0-dev',
    'libjpeg-dev',
    'libltdl-dev',
    'libncurses5-dev',
    'libncursesw5-dev',
    'libnuma-dev',
    'libpixman-1-dev',
    'libpng-dev',
    'libtool',
    'meson',
    'ninja-build',
    'pkg-config',
    'pkg-config',
    'podlators-perl',
    'python3',
    'python3-tomli',
    'texinfo',
    'zlib1g-dev',
]


QEMU_CONFIG_VERSION = [
    ("--disable-auth-pam", (5, 0, 0), None),
    ("--disable-gio", (7, 0, 0), None),
    ("--enable-slirp", (7, 0, 0), None),
    ("--disable-fdt", None, (6, 0, 0)),
    ("--disable-tcmalloc", None, (6, 0, 0)),
    ("--disable-tpm", None, (6, 0, 0)),
    ("--enable-vnc-png", None, (6, 0, 0)),
]

DEFAULT_GIT_TAG = 'yc-5.0'
DEFAULT_OUT_BASENAME = 'qemu-static'
LIBSLIRP_GIT = 'https://gitlab.freedesktop.org/slirp/libslirp.git'
LIBSLIRP_DEFAULT_REF = 'v4.9.3'
QEMU_VERSION_FILES = ('VERSION', 'NEBIUS-VERSION')

GIT_TAG_VERSION_RE = re.compile(
    r'(?:^|[^0-9])(\d+)\.(\d+)(?:\.(\d+))?(?:[-.]?rc(\d+))?(?=$|[^0-9])')


def git_tag_version_key(git_tag):
    match = GIT_TAG_VERSION_RE.search(git_tag or '')
    if match is None:
        return None

    major, minor, patch, rc = match.groups()
    return (
        int(major),
        int(minor),
        int(patch or 0),
        0 if rc else 1,
        int(rc or 0),
    )


def add_config(config, package):
    if package not in config:
        config.append(package)


def has_config(config, package):
    return package in config

def config_matches_version(git_tag, minver, maxver):
    git_tag_version = git_tag_version_key(git_tag)
    if git_tag_version is None:
        return False

    if minver is not None and git_tag_version < tuple(minver):
        return False

    if maxver is not None and git_tag_version >= tuple(maxver):
        return False

    return True


def qemu_target_list():
    if platform.machine().lower() in ("aarch64", "arm64"):
        return "aarch64-softmmu"

    return "x86_64-softmmu"


def qemu_source_version(src_dir):
    for name in QEMU_VERSION_FILES:
        path = os.path.join(src_dir, name)
        if not os.path.exists(path):
            continue

        with open(path) as version_file:
            version = version_file.readline().strip()
            if version:
                return version

    return None


def default_out_path(qemu_version):
    if not qemu_version:
        raise RuntimeError("Cannot derive default --out name; pass --out explicitly")

    tag = re.sub(r'[^A-Za-z0-9._-]+', '_', qemu_version)
    machine = platform.machine().lower()
    return "{}-{}-{}.tgz".format(DEFAULT_OUT_BASENAME, tag, machine)


def pkg_config_variable(package, variable):
    try:
        value = subprocess.check_output(
            ["pkg-config", "--variable=" + variable, package]
        )
    except (OSError, subprocess.CalledProcessError):
        return None

    if isinstance(value, bytes):
        value = value.decode("utf-8")

    return value.strip()


def has_static_slirp():
    libdir = pkg_config_variable("slirp", "libdir")
    return bool(libdir and os.path.exists(os.path.join(libdir, "libslirp.a")))


def prepend_env_path(name, path):
    value = os.environ.get(name)
    if value:
        os.environ[name] = path + os.pathsep + value
    else:
        os.environ[name] = path


def build_static_slirp():
    prefix = os.path.abspath("libslirp-static")
    src_dir = os.path.abspath("libslirp-src")
    build_dir = os.path.abspath("build-libslirp")
    libslirp_git = os.environ.get("LIBSLIRP_GIT", LIBSLIRP_GIT)
    libslirp_ref = os.environ.get("LIBSLIRP_REF", LIBSLIRP_DEFAULT_REF)

    if os.path.exists(src_dir) and not os.path.isdir(os.path.join(src_dir, ".git")):
        raise RuntimeError("libslirp source path is not a git checkout: {}".format(src_dir))

    if not os.path.isdir(os.path.join(src_dir, ".git")):
        run(["git", "clone", libslirp_git, src_dir])

    if libslirp_ref:
        run(["git", "-C", src_dir, "checkout", libslirp_ref])

    if not os.path.exists(prefix):
        os.makedirs(prefix)

    setup_args = [
        build_dir,
        src_dir,
        "--prefix=" + prefix,
        "--libdir=lib",
        "--default-library=static",
    ]

    if os.path.exists(os.path.join(build_dir, "build.ninja")):
        run(["meson", "setup", "--reconfigure"] + setup_args)
    else:
        run(["meson", "setup"] + setup_args)

    run(["ninja", "-C", build_dir, "-j", str(os.sysconf("SC_NPROCESSORS_ONLN"))])
    run(["ninja", "-C", build_dir, "install"])
    prepend_env_path("PKG_CONFIG_PATH", os.path.join(prefix, "lib", "pkgconfig"))


def ensure_static_slirp(config):
    if not has_config(config, "--static") or not has_config(config, "--enable-slirp"):
        return

    if has_static_slirp():
        return

    build_static_slirp()

    if has_static_slirp():
        return

    raise RuntimeError(
        "Static QEMU with -netdev user support requires libslirp.a. "
        "Automatic libslirp build did not make it visible to pkg-config. "
        "Check the build output or build QEMU without --static."
    )


def qemu_config(args, src_dir):
    config = list(QEMU_CONFIG)
    config.append('--target-list=' + qemu_target_list())
    qemu_version = args.git_tag or qemu_source_version(src_dir)

    for package, minver, maxver in QEMU_CONFIG_VERSION:
        if config_matches_version(qemu_version, minver, maxver):
            add_config(config, package)

    return config


def run(args, **kwargs):
    print("+ '" + "' '".join(args) + "'")
    return subprocess.check_call(args, **kwargs)


def install_deps(args):
    run(['sudo', 'apt-get', 'install', '--no-install-recommends', '-y'] + QEMU_DEPS)


def preprocess(args):
    if args.src is None and args.git_tag is None:
        args.git_tag = DEFAULT_GIT_TAG

    if args.src is None:
        args.src = os.path.abspath(
            os.path.join(os.getcwd(), 'qemu-' + args.git_tag if args.git_tag else 'src'))


def checkout(args):
    if not os.path.exists(args.src):
        os.mkdir(args.src)
    else:
        raise RuntimeError("src path already exists {}".format(args.src))

    run(['git', 'clone', args.git, args.src])
    if args.git_tag is not None:
        run(['git', 'checkout', args.git_tag], cwd=args.src)
    run(['git', 'submodule', 'update', '--init', '--recursive'], cwd=args.src)


def build(args):
    with tmpdir(prefix='build-' + os.path.basename(args.src)) as build_dir:
        if os.path.isdir(args.src):
            src_dir = os.path.abspath(args.src)
        else:
            run(['tar', '--strip-components=1', '-xf',
                os.path.abspath(args.src)], cwd=build_dir)
            src_dir = build_dir

        qemu_version = args.git_tag or qemu_source_version(src_dir)
        if args.out is None:
            args.out = default_out_path(qemu_version)

        config = qemu_config(args, src_dir)
        ensure_static_slirp(config)

        help = subprocess.check_output(
            [src_dir + '/configure', '--help'], cwd=build_dir)

        if isinstance(help, bytes):
            help = help.decode("utf-8")

        logging.debug(help)

        # commit b10d49d7619e4957b4b971f816661b57e5061d71
        if 'libssh2' not in help:
            add_config(config, '--disable-libssh')
        else:
            add_config(config, '--disable-libssh2')

        run([src_dir + '/configure'] + config, cwd=build_dir)
        run(['make', '-j', str(os.sysconf('SC_NPROCESSORS_ONLN'))], cwd=build_dir)

        if os.path.isdir(args.out):
            run(['make', 'install', 'DESTDIR=' +
                 os.path.abspath(args.out) +
                 '/qemu'], cwd=build_dir)
        else:
            with tmpdir(prefix='out-' + os.path.basename(args.src)) as out_dir:
                target_dir = os.path.abspath(out_dir) + '/qemu'
                target = os.path.abspath(args.out)

                run(['make', 'install', 'DESTDIR=' + target_dir], cwd=build_dir)
                with tarfile.open(target, 'w:gz') as tar:
                    for x in os.listdir(target_dir):
                        f = os.path.join(target_dir, x)
                        tar.add(f, arcname=os.path.relpath(f, target_dir))


def main(args):
    preprocess(args)

    if args.deps:
        install_deps(args)

    if args.co:
        checkout(args)

    build(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src",
        help="qemu src directory or tarball",
        action="store")
    parser.add_argument(
        "--co",
        help="do checkout qemu source",
        action="store_true",
        default=False)
    parser.add_argument(
        "--git",
        help="source of the qemu",
        action="store",
        default="ssh://git@github.com:qemu/qemu.git")
    parser.add_argument(
        "--git-tag",
        help="specific tag",
        action="store",
        default=None)
    parser.add_argument(
        "--deps",
        help="do install deps",
        action="store_true",
        default=False)
    parser.add_argument(
        "--out",
        help="target directory or tarball; defaults to qemu-static-<version>-<arch>.tgz",
        action="store",
        default=None)

    args = parser.parse_args()
    main(args)
