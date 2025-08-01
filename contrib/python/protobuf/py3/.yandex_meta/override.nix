pkgs: attrs: with pkgs; with python311.pkgs; with attrs; rec {
  pname = "protobuf";
  version = "3.20.3";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-LjQnQpyc/+vyWUkb4K9wGJYH82XC9Bx8N2SvbzNxBfI=";
  };

  prePatch = "";
  patches = [
    build-patch/001-fix-compilation.patch
  ];

  propagatedBuildInputs = [];
}
