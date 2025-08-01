pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.20.1";
  version_underscored = "${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "c-ares";
    repo = "c-ares";
    rev= "cares-${version_underscored}";
    hash = "sha256-FczdEDU9gDVFUVjWGjysuCCR7lB6BRLc6B012v5PXLU=";
  };

  patches = [];

  cmakeFlags = [
    "-DCARES_BUILD_TESTS=OFF"
    "-DCARES_SHARED=ON"
    "-DCARES_STATIC=OFF"
  ];
}
