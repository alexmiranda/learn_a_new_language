{
  inputs = {
    # nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    systems.url = "github:nix-systems/default";
    devenv.url = "github:cachix/devenv";
    devenv.inputs.nixpkgs.follows = "nixpkgs";
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs = { self, nixpkgs, devenv, systems, ... } @ inputs:
    let
      forEachSystem = nixpkgs.lib.genAttrs (import systems);
    in
    {
      packages = forEachSystem (system: {
        devenv-up = self.devShells.${system}.default.config.procfileScript;
      });

      devShells = forEachSystem
        (system:
          let
            pkgs = nixpkgs.legacyPackages.${system};
          in
          {
            default = devenv.lib.mkShell {
              inherit inputs pkgs;
              modules = [
                {
                  # https://devenv.sh/reference/options/
                  packages = with pkgs; [ zig zls hyperfine ];

                  enterShell = ''
                    ${pkgs.hello}/bin/hello
                  '';

                  processes.hello.exec = "hello";
                  scripts.benchmark.exec = ''
                    ${pkgs.gzip}/bin/gzip -d data/payments-1M.jsonl.gz --keep -f && \
                    ${pkgs.hyperfine}/bin/hyperfine \
                      --warmup 3 \
                      --runs 10 \
                      --parameter-list mode fast,safe,small \
                      --setup 'zig build --release={mode}' \
                      'zig-out/bin/learn_a_new_language data/payments-1M.jsonl'
                  '';
                }
              ];
            };
          });
    };
}
