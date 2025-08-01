pkgs: attrs: with pkgs; with attrs; rec {
  version = "2024-05-03";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "11427f31a64b11583fec94b4c2a265c7dafb1ab3";
    hash = "sha256-2KRXK+iLffxzOU0A+iNUeaOtgH6Ndya3l3vzIshArog=";
  };

  patches = [];
}
