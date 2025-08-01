pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.20.2";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-7hLTIujvYIGRqBQgPHrCq0XOh0GJrePBszXJnBFaXVM=";
  };
}

