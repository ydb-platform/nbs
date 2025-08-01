import shutil

from devtools.yamaker.fileutil import subcopy, rename, files
from devtools.yamaker.pathutil import is_c_src
from devtools.yamaker.modules import (
    Program,
    Library,
    Py3Library,
    Switch,
    Linkable,
    Words,
    py_srcs,
)
from devtools.yamaker.project import NixSourceProject


MODULES_WINDOWS = (
    "Modules/_winapi.c",
    "Modules/overlapped.c",
)

MODULES_DARWIN = ("Modules/_scproxy.c",)

MODULES_LINUX = ("Modules/spwdmodule.c",)

MODULES_POSIX = (
    "Modules/_cryptmodule.c",
    "Modules/_posixsubprocess.c",
    "Modules/fcntlmodule.c",
    "Modules/grpmodule.c",
    "Modules/pwdmodule.c",
    "Modules/resource.c",
    "Modules/syslogmodule.c",
    "Modules/termios.c",
)

MODULES_INCLUDED = (
    "Modules/getaddrinfo.c",
    "Modules/getnameinfo.c",
    "Modules/_ssl/cert.c",
    "Modules/_ssl/debughelpers.c",
    "Modules/_ssl/misc.c",
)


def post_build(self):
    for subdir, exclude in (
        ("Include", ()),
        (
            "Modules",
            (
                "_test*.c",
                "_ctypes_test*",
                "xx*",
                "tkappinit.c",
                "readline.c",
                "ossaudiodev.c",
                "nismodule.c",
                "malloc_closure.c",
                "_curses_panel.c",
                "_cursesmodule.c",
                "_dbmmodule.c",
                "_gdbmmodule.c",
                "_tkinter.c",
                "_uuidmodule.c",
                "bench.c",
                "bench_full.c",
                "getpath_noop.c",
            ),
        ),
        ("Objects", ()),
        (
            "PC",
            (
                "_msi.c",
                "_test*.c",
                "_test*.h",
                "config.c",
                "config_minimal.c",
                "dl_nt.c",
                "empty.c",
                "frozen_dllmain.c",
                "launcher.c",
                "launcher2.c",
                "python3dll.c",
            ),
        ),
        ("Parser", ()),
        ("Programs", ("_test*",)),
        (
            "Python",
            (
                "bytecodes.c",
                "dynload_aix.c",
                "dynload_dl.c",
                "dynload_hpux.c",
                "dynload_stub.c",
                "dup2.c",
                "emscripten_signal.c",
                "frozenmain.c",
                "strdup.c",
            ),
        ),
    ):
        subcopy(
            self.srcdir,
            f"{self.dstdir}",
            globs=[f"{subdir}/**/*.[chS]"],
            exclude=exclude,
        )

    rename(self.dstdir + "/Modules/_decimal/libmpdec/io.h", "mpd_io.h")

    subcopy(
        self.srcdir,
        self.dstdir,
        globs=["Lib/**/*.py"],
        exclude=("__phello__.foo.py",),
    )

    subcopy(
        self.srcdir,
        self.dstdir,
        globs=["Lib/venv/scripts/**/*"],
    )

    for subdir in (
        "Lib/__phello__/",
        "Lib/idlelib/",
        "Lib/test/",
        "Lib/tkinter/",
        "Lib/turtledemo/",
        "Modules/_decimal/libmpdec/examples/",
        "Modules/expat/",
        "Modules/_testcapi/",
    ):
        shutil.rmtree(f"{self.dstdir}/{subdir}")

    self.yamakes["bin"] = self.module(
        Program,
        USE_PYTHON3=True,
        PEERDIR=["contrib/tools/python3/Modules/_sqlite"],
        CFLAGS=["-DPy_BUILD_CORE"],
        SRCS=["../Programs/python.c"],
    )
    self.yamakes["bin"].module_args = ["python3"]

    no_lib2to3_srcs = files(f"{self.dstdir}/Lib/lib2to3", rel=f"{self.dstdir}/Lib")
    self.yamakes["Lib"] = self.module(
        Py3Library,
        PEERDIR=["certs", "contrib/tools/python3/lib2/py"],
        PY_SRCS=sorted(py_srcs(f"{self.dstdir}/Lib", remove=no_lib2to3_srcs) + ["_sysconfigdata_arcadia.py"]),
        NO_LINT=True,
        NO_PYTHON_INCLUDES=True,
    )
    self.yamakes["Lib"].before("PY3_LIBRARY", "ENABLE(PYBUILD_NO_PY)\n")

    sqlite_srcs = files(f"{self.dstdir}/Modules/_sqlite", rel=True, test=is_c_src)
    self.yamakes["Modules/_sqlite"] = self.module(
        Library,
        PEERDIR=["contrib/libs/sqlite3"],
        ADDINCL=[
            "contrib/libs/sqlite3",
            "contrib/tools/python3/Include",
            "contrib/tools/python3/Include/internal",
        ],
        CFLAGS=["-DMODULE_NAME=sqlite3"],
        SRCS=sqlite_srcs,
        PY_REGISTER=["_sqlite3"],
        NO_COMPILER_WARNINGS=True,
        NO_RUNTIME=True,
        PYTHON3_ADDINCL=True,
    )

    modules_srcs = files(f"{self.dstdir}/Modules", rel=self.dstdir, test=is_c_src) + ["Modules/config.c"]
    modules_srcs = filter(lambda x: not x.startswith("Modules/_blake2/impl/"), modules_srcs)
    modules_srcs = filter(lambda x: not x.startswith("Modules/_sha3/kcp/"), modules_srcs)
    modules_srcs = filter(lambda x: not x.startswith("Modules/_sqlite/"), modules_srcs)
    modules_srcs = filter(lambda x: x not in MODULES_WINDOWS, modules_srcs)
    modules_srcs = filter(lambda x: x not in MODULES_DARWIN, modules_srcs)
    modules_srcs = filter(lambda x: x not in MODULES_LINUX, modules_srcs)
    modules_srcs = filter(lambda x: x not in MODULES_POSIX, modules_srcs)
    modules_srcs = filter(lambda x: x not in MODULES_INCLUDED, modules_srcs)

    src_srcs = files(f"{self.dstdir}/Objects", rel=self.dstdir, test=is_c_src)
    src_srcs += files(f"{self.dstdir}/Parser", rel=self.dstdir, test=is_c_src)
    src_srcs += files(f"{self.dstdir}/Python", rel=self.dstdir, test=is_c_src)
    src_pc_srcs = files(f"{self.dstdir}/PC", rel=self.dstdir, test=is_c_src)
    src_srcs.remove("Python/dynload_shlib.c")
    src_srcs.remove("Python/dynload_win.c")
    src_srcs.append("Python/deepfreeze/deepfreeze.c")
    src_srcs.extend(modules_srcs)

    self.yamakes["."] = self.module(
        Library,
        PEERDIR=[
            "contrib/libs/expat",
            "contrib/libs/libbz2",
            # libc_compat is needed in order to make <sys/random.h> resolvable
            "contrib/libs/libc_compat",
            "contrib/libs/openssl",
            "contrib/libs/lzma",
            "contrib/libs/zlib",
            "contrib/restricted/libffi",
            "library/cpp/sanitizer/include",
        ],
        ADDINCL=[
            "contrib/libs/expat",
            "contrib/libs/libbz2",
            "contrib/restricted/libffi/include",
            "contrib/tools/python3/Include",
            "contrib/tools/python3/Include/internal",
            "contrib/tools/python3/Modules",
            "contrib/tools/python3/Modules/_decimal/libmpdec",
            "contrib/tools/python3/Modules/_hacl/include",
            "contrib/tools/python3/PC",
        ],
        CFLAGS=[
            "-DPy_BUILD_CORE",
            "-DPy_BUILD_CORE_BUILTIN",
        ],
        SRCS=src_srcs,
        NO_COMPILER_WARNINGS=True,
        NO_UTIL=True,
        SUPPRESSIONS=["tsan.supp"],
    )
    self.yamakes["."].after(
        "CFLAGS",
        Switch(
            CLANG_CL=Linkable(CFLAGS=["-Wno-invalid-token-paste"]),
        ),
    )
    darwin = Linkable(
        LDFLAGS=[
            Words("-framework", "CoreFoundation"),
            Words("-framework", "SystemConfiguration"),
        ]
    )
    windows = Linkable(
        CFLAGS=['-DPY3_DLLNAME=L"python3"'],
        LDFLAGS=["Mincore.lib", "Shlwapi.lib", "Winmm.lib"],
    )
    windows.after("LDFLAGS", "# DISABLE(MSVC_INLINE_OPTIMIZED)")
    self.yamakes["."].after(
        "CFLAGS",
        Switch(
            OS_DARWIN=darwin,
            OS_WINDOWS=windows,
        ),
    )

    self.yamakes["."].after(
        "SRCS",
        Switch(
            OS_WINDOWS=Linkable(SRCS=MODULES_WINDOWS + tuple(src_pc_srcs) + ("Python/dynload_win.c",)),
            default=Linkable(SRCS=MODULES_POSIX + ("Python/dynload_shlib.c",)),
        ),
    )

    linux = Linkable(SRCS=MODULES_LINUX + ("Python/asm_trampoline.S",))
    linux.before(
        "SRCS",
        Switch({"NOT MUSL": Linkable(EXTRALIBS=["crypt"])}),
    )
    self.yamakes["."].after(
        "SRCS",
        Switch(
            OS_LINUX=linux,
            OS_DARWIN=Linkable(SRCS=MODULES_DARWIN),
        ),
    )

    for name, yamake in self.yamakes.items():
        yamake.LICENSE = ["Python-2.0"]

    self.yamakes["."].RECURSE = sorted(key for key in self.yamakes if key != ".")


python3 = NixSourceProject(
    arcdir="contrib/tools/python3",
    nixattr="python3",
    owners=["g:python-contrib"],
    keep_paths=[
        "a.yaml",
        "lib2",
        "Include/pyconfig*.h",
        "Lib/_sysconfigdata_arcadia.py",
        "Modules/config.c",
        "Python/deepfreeze",
        "Python/frozen_modules",
        "tsan.supp",
    ],
    disable_includes=[
        "pydtrace_probes.h",
        "blake2-kat.h",
        "bluetooth/",
        "bluetooth.h",
        "displayIntermediateValues.h",
        "iconv.h",
        "KeccakP-200-SnP.h",
        "KeccakP-400-SnP.h",
        "KeccakP-800-SnP.h",
        "netcan/can.h",
        "emscripten.h",
        "bits/alltypes.h",
        "sys/byteorder.h",
        "sys/lwp.h",
        # ifdef __VXWORKS__
        "rtpLib.h",
        "taskLib.h",
        "vxCpuLib.h",
    ],
    copy_sources=[
        "Modules/**/*.macros",
        "Modules/**/*.inc",
        "Objects/**/*.inc",
    ],
    post_build=post_build,
)
