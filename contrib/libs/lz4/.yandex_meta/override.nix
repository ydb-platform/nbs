self: super: with self; rec {
  version = "1.9.4";

  src = fetchFromGitHub {
    owner = "lz4";
    repo = "lz4";
    rev = "v${version}";
    hash = "sha256-YiMCD3vvrG+oxBUghSrCmP2LAfAGZrEaKz0YoaQJhpI=";
  };

  patches = [];
}
