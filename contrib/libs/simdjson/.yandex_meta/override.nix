pkgs: attrs: with pkgs; rec {
  version = "3.9.4";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-iTlIdLNOr1rRBnCwnI34e2RLL18Fmc/kRSMdZcOLp98=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
