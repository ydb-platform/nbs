pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.0";

  src = fetchurl {
    url = "mirror://gnu/${pname}/${pname}-${version}.tar.gz";
    sha256 = "sha256-5bfsddC5omVdjRhUxKJniaNw24d373eHTdRT7RgUgtI=";
  };

  patches = [
    ./support-glibc-2.28.patch
  ];

  configureFlags = [
    "--disable-nls"
  ];
}
