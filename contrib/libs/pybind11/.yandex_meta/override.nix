pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.12.0";

  src = fetchFromGitHub {
    owner = "pybind";
    repo = "pybind11";
    rev = "v${version}";
    hash = "sha256-DVkI5NxM5uME9m3PFYVpJOOa2j+yjL6AJn76fCTv2nE=";
  };

  patches = [];
}

