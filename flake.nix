{
  description = "NBS development shell and Nix checks for syncing YDB stable-26-3-1";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";

    ydb-src = {
      url = "github:ydb-platform/ydb/ffa7b99b42391d01548c55bb7117d61a0e74fc63";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, ydb-src }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      mkPkgs = system: import nixpkgs {
        inherit system;
        config = {
          allowUnfree = false;
        };
      };
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system (mkPkgs system));

      syncSteps = [
        {
          script = "tools/ydb-sync/10-copy-folded-layout.sh";
          purpose = "copy upstream ydb plus folded yql/essentials, yql/providers, and yt/yql/providers";
        }
        {
          script = "tools/ydb-sync/20-apply-import-contrib-patches.sh";
          purpose = "reuse vendored import_contrib patch scripts for ya.make, sources, python, and protos";
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

      mkSyncOrder = pkgs: pkgs.writeText "ydb-sync-order.txt" (
        nixpkgs.lib.concatMapStringsSep "\n" (step: "${step.script}: ${step.purpose}") syncSteps
      );

      mkSyncedSrc = pkgs: pkgs.stdenvNoCC.mkDerivation {
        pname = "nbs-ydb-synced-src";
        version = "stable-26-3-1";

        src = ./.;

        nativeBuildInputs = with pkgs; [
          bash
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

        dontConfigure = true;
        dontFixup = true;

        buildPhase = ''
          runHook preBuild

          chmod -R u+w .
          export HOME="$TMPDIR"
          export ROOT="$PWD"
          export ARC_ROOT="$PWD"
          export ALLOW_DIRTY=1
          export YDB_SRC=${ydb-src}
          export IMPORT_CONTRIB_DIR="$PWD/tools/ydb-sync/import-contrib"

          patchShebangs ./ya tools/sync-ydb-stable-26-3-1.sh tools/ydb-sync tools/ydb-sync/import-contrib
          tools/sync-ydb-stable-26-3-1.sh

          runHook postBuild
        '';

        installPhase = ''
          runHook preInstall

          mkdir -p "$out"
          cp -R . "$out/"

          runHook postInstall
        '';
      };

      mkYaBootstrapResource = pkgs: { pname, resourceId, hash, md5Prefix, executable }: pkgs.stdenvNoCC.mkDerivation {
        inherit pname;
        version = resourceId;

        src = pkgs.fetchurl {
          url = "https://devtools-registry.s3.yandex.net/${resourceId}";
          inherit hash;
        };

        nativeBuildInputs = with pkgs; [
          gnutar
          gzip
        ];

        dontUnpack = true;
        dontConfigure = true;
        dontBuild = true;
        dontFixup = true;

        installPhase = ''
          runHook preInstall

          mkdir -p "$out/${md5Prefix}_d"
          tar -xzf "$src" -C "$out/${md5Prefix}_d"
          chmod +x "$out/${md5Prefix}_d/${executable}"

          runHook postInstall
        '';
      };

      mkYaToolResource = pkgs: { pname, resourceId, hash, executable }: pkgs.stdenvNoCC.mkDerivation {
        inherit pname;
        version = resourceId;

        src = pkgs.fetchurl {
          url = "https://devtools-registry.s3.yandex.net/${resourceId}";
          inherit hash;
        };

        nativeBuildInputs = with pkgs; [
          autoPatchelfHook
          gnutar
          gzip
        ];

        buildInputs = with pkgs; [
          glibc
          libxcrypt-legacy
          stdenv.cc.cc.lib
        ];

        dontUnpack = true;
        dontConfigure = true;
        dontBuild = true;

        installPhase = ''
          runHook preInstall

          mkdir -p "$out/v4/${resourceId}"
          tar -xzf "$src" -C "$out/v4/${resourceId}"
          chmod +x "$out/v4/${resourceId}/${executable}"
          printf '{"file_name": "${resourceId}", "id": "${resourceId}"}' > "$out/v4/${resourceId}/resource_info.json"
          printf 'sbr:${resourceId}' > "$out/v4/${resourceId}/lnk"
          printf '2' > "$out/v4/${resourceId}/INSTALLED"

          runHook postInstall
        '';
      };

      mkYaBootstrapTools = pkgs: mkYaBootstrapResource pkgs {
        pname = "ya-bootstrap-tools";
        resourceId = "8580483288";
        hash = "sha256-w4wudSQAvx7Tck4fA/nRdIOw+vT1hfkU5XGrmU8tCGo=";
        md5Prefix = "24ab5119e2";
        executable = "ya-bin";
      };

      mkYmakeTool = pkgs: mkYaToolResource pkgs {
        pname = "ymake-tool";
        resourceId = "6547534096";
        hash = "sha256-8uq5z7oLRC0rXZaWyw8K0ezIUgYHsYOLtBWYaoMnwvM=";
        executable = "ymake";
      };

      mkYaTcTool = pkgs: mkYaToolResource pkgs {
        pname = "ya-tc-tool";
        resourceId = "6512096202";
        hash = "sha256-mI1qPLq5h8Mk8JI3vFaUJG0uujYZSuw0Og12KP8xkYc=";
        executable = "ya-tc";
      };

      mkYaPrefetchedTools = system: pkgs: pkgs.symlinkJoin {
        name = "ya-prefetched-tools";
        paths = [
          self.packages.${system}.ya-bootstrap-tools
          self.packages.${system}.ymake-tool
          self.packages.${system}.ya-tc-tool
        ];
      };

      mkYaMakeCheck = system: pkgs: name: yaArgs: pkgs.stdenvNoCC.mkDerivation {
        inherit name;

        src = self.packages.${system}.nbs-ydb-synced-src;

        nativeBuildInputs = with pkgs; [
          bash
          coreutils
          git
          perl
          python3
        ];

        dontConfigure = true;
        dontFixup = true;

        buildPhase = ''
          runHook preBuild

          chmod -R u+w .
          export HOME="$TMPDIR"
          export YA_CACHE_DIR="$TMPDIR/ya-cache"
          export YA_CACHE_DIR_TOOLS="$TMPDIR/ya-cache-tools"
          export YA_TOOL_ROOT="$TMPDIR/ya-tool-root"

          mkdir -p "$YA_CACHE_DIR_TOOLS"
          cp -R "${self.packages.${system}.ya-prefetched-tools}/." "$YA_CACHE_DIR_TOOLS/"
          chmod -R u+w "$YA_CACHE_DIR_TOOLS"

          patchShebangs ./ya
          ./ya make -r ${yaArgs}

          runHook postBuild
        '';

        installPhase = ''
          runHook preInstall

          mkdir -p "$out"
          touch "$out/${name}"

          runHook postInstall
        '';
      };
    in {
      overlays.default = final: prev: {
        nbs-ydb-synced-src = mkSyncedSrc final;
      };

      packages = forAllSystems (system: pkgs:
        let
          pkgsWithOverlay = import nixpkgs {
            inherit system;
            overlays = [ self.overlays.default ];
            config = {
              allowUnfree = false;
            };
          };
        in {
          ydb-sync-order = mkSyncOrder pkgs;
          nbs-ydb-synced-src = pkgsWithOverlay.nbs-ydb-synced-src;
          ya-bootstrap-tools = mkYaBootstrapTools pkgs;
          ymake-tool = mkYmakeTool pkgs;
          ya-tc-tool = mkYaTcTool pkgs;
          ya-prefetched-tools = mkYaPrefetchedTools system pkgs;
        });

      apps = forAllSystems (system: pkgs: {
        ya-make-smoke = {
          type = "app";
          program = "${pkgs.writeShellScript "ya-make-smoke" ''
            exec ./ya make -r cloud/storage/core/libs/actors
          ''}";
        };

        ya-make-actors-ut = {
          type = "app";
          program = "${pkgs.writeShellScript "ya-make-actors-ut" ''
            exec ./ya make -r -t cloud/storage/core/libs/actors/ut
          ''}";
        };
      });

      checks = forAllSystems (system: pkgs: {
        ya-make-smoke = mkYaMakeCheck system pkgs "ya-make-smoke" "cloud/storage/core/libs/actors";
        ya-make-actors-ut = mkYaMakeCheck system pkgs "ya-make-actors-ut" "-t cloud/storage/core/libs/actors/ut";
      });

      devShells = forAllSystems (system: pkgs: {
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
            sed 's/^/  - /' ${self.packages.${system}.ydb-sync-order}
            echo "Run local sync: tools/sync-ydb-stable-26-3-1.sh"
            echo "Build synced source: nix build .#nbs-ydb-synced-src"
            echo "Run ya checks: nix flake check"
          '';
        };
      });
    };
}
