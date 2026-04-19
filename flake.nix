{
  description = "signal-compute: quarterly disproportionality signals from faers-pipeline contingency";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
      in {
        devShells.default = pkgs.mkShell {
          name = "signal-compute";

          packages = [
            pkgs.R
            pkgs.pkg-config
            pkgs.openssl
            pkgs.libxml2
            pkgs.zlib
            pkgs.libuv
            pkgs.icu
            pkgs.git
            pkgs.cmake
          ];

          shellHook = ''
            export USER=''${USER:-$(whoami)}
            export LANG=C.UTF-8
            export LC_ALL=C.UTF-8
            export R_LIBS_USER="/home/harlan/R/x86_64-pc-linux-gnu-library/4.5"
            mkdir -p "$R_LIBS_USER"

            echo ""
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "  signal-compute dev env"
            echo "  R: $(R --version | head -1)"
            echo "  Run: Rscript R/compute_quarterly.R"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
          '';
        };
      });
}
