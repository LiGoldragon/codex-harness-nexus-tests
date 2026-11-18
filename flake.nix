{
  description = "Codex Harness Nexus behavioral tests.";
  inputs.base.url = "github:LiGoldragon/build-base";
  # The Nexus whose pair the tests run, at the revision its crates are
  # taken at through Cargo, built on this base.
  inputs.codex-harness-nexus = {
    url = "github:LiGoldragon/codex-harness-nexus/e178db0a559f9740e16b3d70ac40d46a6ae4166b";
    inputs.base.follows = "base";
  };
  outputs =
    { self, base, codex-harness-nexus }:
    base.lib.workspaceOutputs {
      cargoNix = ./Cargo.nix;
      # The pair tests start the packaged Nexus, named to them by its path.
      memberChecks =
        { pkgs, check, ... }:
        {
          codex-harness-nexus-tests = (check "codex-harness-nexus-tests" [ "default" ]).overrideAttrs {
            CODEX_HARNESS_NEXUS = codex-harness-nexus.packages.${pkgs.stdenv.hostPlatform.system}.default;
          };
        };
      extraChecks =
        { pkgs, toolchain, ... }:
        {
          fmt = pkgs.runCommand "codex-harness-nexus-tests-fmt" { nativeBuildInputs = [ toolchain ]; } ''
            rustfmt --edition 2024 --check ${./src}/lib.rs ${./tests}/transitions.rs ${./tests}/nexus.rs ${./tests}/pair.rs
            touch "$out"
          '';
        };
    };
}
