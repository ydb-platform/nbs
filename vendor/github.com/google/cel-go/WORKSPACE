workspace(name = "cel_go")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "099a9fb96a376ccbbb7d291ed4ecbdfd42f6bc822ab77ae6f1b5cb9e914e94fa",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.35.0/rules_go-v0.35.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.35.0/rules_go-v0.35.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "ecba0f04f96b4960a5b250c8e8eeec42281035970aa8852dda73098274d14a1d",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.29.0/bazel-gazelle-v0.29.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.29.0/bazel-gazelle-v0.29.0.tar.gz",
    ],
)

# rules_proto v3.20.0
http_archive(
    name = "rules_proto",
    sha256 = "e017528fd1c91c5a33f15493e3a398181a9e821a804eb7ff5acdd1d2d6c2b18d",
    strip_prefix = "rules_proto-4.0.0-3.20.0",
    urls = [
        "https://github.com/bazelbuild/rules_proto/archive/refs/tags/4.0.0-3.20.0.tar.gz",
    ],
)

# googleapis as of 08/31/2023
http_archive(
    name = "com_google_googleapis",
    sha256 = "5c56500adf7b1b7a3a2ee5ca5b77500617ad80afb808e3d3979f582e64c0523d",
    strip_prefix = "googleapis-25f99371444ea7fd0dc1523ca6925e91cc48a664",
    urls = [
        "https://github.com/googleapis/googleapis/archive/25f99371444ea7fd0dc1523ca6925e91cc48a664.tar.gz",
    ],
)

# protobuf
http_archive(
    name = "com_google_protobuf",
    sha256 = "8242327e5df8c80ba49e4165250b8f79a76bd11765facefaaecfca7747dc8da2",
    strip_prefix = "protobuf-3.21.5",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.21.5.zip"],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")
load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")
load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
    go = True,
)

# Do *not* call *_dependencies(), etc, yet.  See comment at the end.

go_repository(
    name = "org_golang_google_protobuf",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/protobuf",
    tag = "v1.28.1",
)

# Generated Google APIs protos for Golang 08/03/2023
go_repository(
    name = "org_golang_google_genproto_googleapis_api",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/genproto/googleapis/api",
    sum = "h1:nIgk/EEq3/YlnmVVXVnm14rC2oxgs1o0ong4sD/rd44=",
    version = "v0.0.0-20230803162519-f966b187b2e5",
)

# Generated Google APIs protos for Golang 08/03/2023
go_repository(
    name = "org_golang_google_genproto_googleapis_rpc",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/genproto/googleapis/rpc",
    sum = "h1:eSaPbMR4T7WfH9FvABk36NBMacoTUKdWCvV0dx+KfOg=",
    version = "v0.0.0-20230803162519-f966b187b2e5",
)

# gRPC deps for v1.49.0 (including x/text and x/net)
go_repository(
    name = "org_golang_google_grpc",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/grpc",
    tag = "v1.49.0",
)

go_repository(
    name = "org_golang_x_net",
    importpath = "golang.org/x/net",
    sum = "h1:oWX7TPOiFAMXLq8o0ikBYfCJVlRHBcsciT5bXOrH628=",
    version = "v0.0.0-20190311183353-d8887717615a",
)

go_repository(
    name = "org_golang_x_text",
    importpath = "golang.org/x/text",
    sum = "h1:olpwvP2KacW1ZWvsR7uQhoyTYvKAupfQrRGBFM352Gk=",
    version = "v0.3.7",
)

# ANTLR v4.13.0
go_repository(
    name = "com_github_antlr4_go_antlr_v4",
    importpath = "github.com/antlr4-go/antlr/v4",
    sum = "h1:lxCg3LAv+EUK6t1i0y1V6/SLeUi0eKEKdhQAlS8TVTI=",
    version = "v4.13.0",
)

# CEL Spec deps
go_repository(
    name = "com_google_cel_spec",
    commit = "dd5373cc157f6325421470139b5d55baed09935a",
    importpath = "github.com/google/cel-spec",
)

# strcase deps
go_repository(
    name = "com_github_stoewer_go_strcase",
    importpath = "github.com/stoewer/go-strcase",
    sum = "h1:Z2iHWqGXH00XYgqDmNgQbIBxf3wrNq0F3feEy0ainaU=",
    version = "v1.2.0",
)

# Readline for repl
go_repository(
    name = "com_github_chzyer_readline",
    commit = "62c6fe6193755f722b8b8788aa7357be55a50ff1",  # v1.4
    importpath = "github.com/chzyer/readline",
)

# golang.org/x/exp deps
go_repository(
    name = "org_golang_x_exp",
    importpath = "golang.org/x/exp",
    sum = "h1:+WEEuIdZHnUeJJmEUjyYC2gfUMj69yZXw17EnHg/otA=",
    version = "v0.0.0-20220722155223-a9213eeb770e",
)

# Run the dependencies at the end.  These will silently try to import some
# of the above repositories but at different versions, so ours must come first.
go_rules_dependencies()

go_register_toolchains(version = "1.19.1")

gazelle_dependencies()

rules_proto_dependencies()

rules_proto_toolchains()

protobuf_deps()
