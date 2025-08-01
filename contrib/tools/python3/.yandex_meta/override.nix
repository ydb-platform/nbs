pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.12.3";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-Eg9tnDGoZL9DBO1o9n5cc8WJN7dqB4CeR5COfzBGbXY=";
  };

  patches = [];
  postPatch = "";
}
