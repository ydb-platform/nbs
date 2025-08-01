pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.6.2";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-CpyDi8PR7yF+Mszc/LqMNeBAZavoDoAQ/dQYHBKLXeM=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}
