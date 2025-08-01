import os
import itertools

from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker import pathutil
from devtools.yamaker.project import GNUMakeNixProject


WINDOWS_SRCS = [
    "close.c",
    "dup2.c",
    "error.c",
    "fcntl.c",
    "fpending.c",
    "getdtablesize.c",
    "getopt.c",
    "getopt1.c",
    "getopt_int.h",
    "isnand-nolibm.h",
    "isnanf-nolibm.h",
    "msvc-inval.c",
    "msvc-inval.h",
    "msvc-nothrow.c",
    "msvc-nothrow.h",
    "obstack.c",
    "obstack_printf.c",
    "open.c",
    "raise.c",
    "sigaction.c",
    "sigprocmask.c",
    "stpcpy.c",
    "strndup.c",
    "strverscmp.c",
    "unitypes.h",
    "uniwisth.h",
    "w32spawn.h",
    "waitpid.c",
    "wcwidth.c",
]

DARWIN_SRCS = [
    "error.c",
    "fpending.c",
    "obstack.c",
    "obstack_printf.c",
    "strverscmp.c",
]

WINDOWS_HEADERS = [
    "alloca.h",
]

EXCESSIVE_SRCS = [
    "alloca.h",
    "fcntl.h",
    "fprintf.c",
    "inttypes.h",
    "math.h",
    "printf.c",
    "signal.h",
    "snprintf.c",
    "spawn.h",
    "sprintf.c",
    "stdio.h",
    "stdlib.h",
    "strerror_r.c",
    "string.h",
    "sys/types.h",
    "sys/wait.h",
    "time.h",
    "unistd.h",
    "vfprintf.c",
    "vsnprintf.c",
    "vsprintf.c",
    "wchar.h",
    "wctype.h",
]


def post_install(self):
    with self.yamakes["lib"] as gnulib:
        gnulib.after(
            "SRCS",
            Switch(
                OS_WINDOWS=Linkable(
                    SRCS=[src for src in WINDOWS_SRCS if pathutil.is_source(src)],
                    ADDINCL=[GLOBAL(f"{self.arcdir}/lib/platform/win64")],
                ),
                OS_DARWIN=Linkable(
                    SRCS=DARWIN_SRCS,
                ),
            ),
        )
        for src in WINDOWS_SRCS:
            if pathutil.is_source(src) and src in gnulib.SRCS:
                gnulib.SRCS.remove(src)

        for src in EXCESSIVE_SRCS:
            os.remove(f"{self.dstdir}/lib/{src}")
            if pathutil.is_source(src):
                gnulib.SRCS.remove(src)

    with self.yamakes["."] as bison:
        bison.SRCS.add("arcadia_root.cpp.in")
        bison.CFLAGS.append('-DBISON_DATA_DIR=contrib/tools/bison/data')


bison = GNUMakeNixProject(
    arcdir="contrib/tools/bison",
    owners=["g:cpp-contrib"],
    nixattr="bison",
    ignore_commands=[
        "bash",
        "cat",
        "sed",
    ],
    use_full_libnames=True,
    install_targets=[
        "bison",
        "libbison",
    ],
    put={
        "bison": ".",
        "libbison": "lib",
    },
    keep_paths=[
        "arcadia_root.h",
        "arcadia_root.cpp.in",
        "lib/platform/win64/*.h",
        "lib/platform/win64/sys/*.h",
        # Keep this for now as upstream code crashes on Windows
        "src/scan-skel.c",
    ],
    copy_sources=[
        "data/*.c",
        "data/*.cc",
        "data/*.hh",
        "data/*.m4",
        "data/m4sugar/*.m4",
        # These lex / bison sources will not be used
        # (how does one bootstrap bison without having bison?)
        #
        # Just copy them for informational purposes
        "src/scan-code.l",
        "src/scan-gram.l",
        "src/scan-skel.l",
        "src/parse-gram.y",
    ]
    + [f"lib/{src}" for src in itertools.chain(WINDOWS_SRCS, WINDOWS_HEADERS, DARWIN_SRCS)],
    copy_sources_except=[
        # Don't need them for now, reduce import footprint
        "data/glr.c",
        "data/java.m4",
        "data/java-skel.m4",
    ],
    platform_dispatchers=[
        "lib/config.h",
        "lib/configmake.h",
    ],
    disable_includes=[
        "synch.h",
        "random.h",
        "OS.h",
        "os2.h",
    ],
    post_install=post_install,
)

bison.copy_top_sources_except |= {
    "ABOUT-NLS",
    "ChangeLog",
    "ChangeLog-2012",
    "ChangeLog-1998",
    "INSTALL",
}
