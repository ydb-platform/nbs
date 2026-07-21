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
      syncSteps = [
        {
          script = "tools/ydb-sync/10-copy-folded-layout.sh";
          purpose = "copy upstream ydb plus folded yql/essentials, yql/providers, and yt/yql/providers";
        }
        {
          script = "tools/ydb-sync/20-apply-import-contrib-patches.sh";
          purpose = "reuse existing import_contrib patch scripts for ya.make, sources, python, and protos";
        }
        {
          script = "tools/ydb-sync/30-rewrite-paths.sh";
          purpose = "rewrite new YQL/YT layout paths to contrib/ydb/library/yql and apply local ya compatibility rewrites";
        }
        {
          script = "tools/ydb-sync/40-prune-contrib-ydb.sh";
          purpose = "exclude docs and other YDB subtrees not needed by the current NBS import";
        }
        {
          script = "tools/ydb-sync/50-copy-extra-deps.sh";
          purpose = "copy selected stable-26-3-1 third-party and library dependencies";
        }
      ];
    in {
      packages = forAllSystems (pkgs: {
        ydb-sync-order = pkgs.writeText "ydb-sync-order.txt" (
          nixpkgs.lib.concatMapStringsSep "\n" (step: "${step.script}: ${step.purpose}") syncSteps
        );
      });

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
            echo "Sync order:"
            sed 's/^/  - /' ${self.packages.${pkgs.system}.ydb-sync-order}
            echo "Run: tools/sync-ydb-stable-26-3-1.sh"
          '';
        };
      });
    };
}
