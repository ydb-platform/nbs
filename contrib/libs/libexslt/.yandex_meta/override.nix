pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.1.39";

  nativeBuildInputs = [ pkg-config autoreconfHook ];

  src = fetchFromGitLab {
    domain = "gitlab.gnome.org";
    owner = "GNOME";
    repo = "libxslt";
    rev = "v${version}";
    hash = "sha256-4t5dcHAMxcbJg8hBmCZJF6co2UKrjq9SrsMzEhmY2Qs=";
  };
}
