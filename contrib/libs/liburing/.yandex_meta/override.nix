pkgs: attrs: with pkgs; with attrs; rec {
  name = "liburing";
  version = "2.6";

  src = fetchFromGitHub {
    owner = "axboe";
    repo = "liburing";
    rev    = "liburing-${version}";
    hash = "sha256-UOhnFT4UKZmPchKxew3vYeKH2oETDVylE1RmJ2hnLq0=";
  };

  buildPhase = ''
    make -j$(nproc) -C src
    make -j$(nproc) -C test
  '';

  patches = [];
}
