self: super: with self; rec {
  version = "20240116.2";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-eA2/dZpNOlex1O5PNa3XSZhpMB3AmaIoHzVDI9TD/cg=";
  };

  patches = [];
}
