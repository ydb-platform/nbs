{
  description = "NBS development shell for syncing YDB stable-26-3-1";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f (import nixpkgs {
        inherit system;
        config = {
          allowUnfree = false;
        };
      }));
    in {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            bashInteractive
            coreutils
            diffutils
            findutils
            gawk
            git
            gnugrep
            gnused
            perl
            python3
            rsync
          ];

          shellHook = ''
            echo "NBS/YDB sync shell"
            echo "Run: tools/sync-ydb-stable-26-3-1.sh"
          '';
        };
      });
    };
}
