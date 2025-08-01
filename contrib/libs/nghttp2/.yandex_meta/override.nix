pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.62.1";

  src = fetchFromGitHub {
    owner = "nghttp2";
    repo = "nghttp2";
    rev = "v${version}";
    hash = "sha256-56+u4P6/YO7aF62jsYUZL712ANVIkSoKXEj3zH89fzY=";
  };

  patches = [];

  # Add autoreconfHook to run ./autogen.sh during preConfigure stage
  nativeBuildInputs = [ autoreconfHook pkg-config ];
}
