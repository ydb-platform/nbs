pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.0.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-Au5s/0gOEdM/o+cajN+1720gx8aK1OODqSxqN3w0BCY=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}
